package dev.stockreading.collector.marketdata;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 현재가가 같은 응답의 전일대비·등락률·고저가와 맞는지 본다. 시세 제공자가 가끔 현재가 칸만 엉뚱한 값으로 보내는데,
 * 그 응답의 전일대비·등락률은 원래 현재가로 계산돼 오므로 서로 어긋난다. 비교 칸도 따로 깨질 수 있어
 * 판단할 수 있는 검사 중 과반이 어긋날 때만 버린다.
 */
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
class PriceFieldCheck {

    /** 걸린 검사 이름. 실패 이벤트의 exceptionType 에 "FieldMismatch:CHANGE+RATE" 처럼 담긴다. */
    enum Check {
        CHANGE,
        RATE,
        HIGH_LOW
    }

    static final String EXCEPTION_PREFIX = "FieldMismatch:";
    // 등락률은 소수 둘째 자리라 반올림 폭(0.005%p)만큼 더 허용한다.
    private static final BigDecimal RATE_ROUNDING = new BigDecimal("0.00005");
    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);
    private static final Logger log = LoggerFactory.getLogger(PriceFieldCheck.class);

    private enum Result {
        OK,
        BAD,
        UNKNOWN
    }

    private final PriceCheckProperties properties;
    private final ObjectMapper mapper;

    PriceFieldCheck(PriceCheckProperties properties, ObjectMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
        if (properties.enabled() && properties.noFieldsConfigured()) {
            log.warn("price check is on but no field names are set; every quote passes unchecked");
        }
    }

    /** 버려야 하면 걸린 검사를, 통과면 빈 목록을 돌려준다. 판단할 수 있는 검사가 없으면 통과다. */
    List<Check> failedChecks(long price, String raw) {
        if (!properties.enabled() || raw == null || raw.isBlank()) {
            return List.of();
        }
        JsonNode body;
        try {
            body = mapper.readTree(raw).path("dataBody");
        } catch (JacksonException e) {
            return List.of();
        }
        BigDecimal now = BigDecimal.valueOf(price);
        List<BigDecimal> bases = new ArrayList<>();
        for (String field : properties.baseFields()) {
            BigDecimal base = number(body, field);
            if (base != null && base.signum() > 0) {
                bases.add(base);
            }
        }
        Map<Check, Result> results = new EnumMap<>(Check.class);
        results.put(Check.CHANGE, change(body, now, bases));
        results.put(Check.RATE, rate(body, now, bases));
        results.put(Check.HIGH_LOW, highLow(body, now));

        List<Check> failed = new ArrayList<>();
        int decided = 0;
        for (Map.Entry<Check, Result> entry : results.entrySet()) {
            if (entry.getValue() != Result.UNKNOWN) {
                decided++;
            }
            if (entry.getValue() == Result.BAD) {
                failed.add(entry.getKey());
            }
        }
        return decided > 0 && failed.size() * 2 > decided ? List.copyOf(failed) : List.of();
    }

    static String exceptionType(List<Check> failed) {
        return EXCEPTION_PREFIX + String.join("+", failed.stream().map(Enum::name).toList());
    }

    // 현재가 = 전일 종가 ± 전일대비. 전일 종가 칸 중 하나라도 맞으면 통과한다.
    private Result change(JsonNode body, BigDecimal now, List<BigDecimal> bases) {
        BigDecimal diff = number(body, properties.changeField());
        Integer sign = sign(text(body, properties.changeSignField()));
        if (diff == null || sign == null || bases.isEmpty()) {
            return Result.UNKNOWN;
        }
        for (BigDecimal base : bases) {
            BigDecimal expected = base.add(diff.abs().multiply(BigDecimal.valueOf(sign)));
            if (now.subtract(expected).abs().compareTo(expected.abs().multiply(properties.tolerance())) <= 0) {
                return Result.OK;
            }
        }
        return Result.BAD;
    }

    // 현재가 = 전일 종가 × (1 + 등락률/100).
    private Result rate(JsonNode body, BigDecimal now, List<BigDecimal> bases) {
        BigDecimal rate = number(body, properties.rateField());
        if (rate == null || bases.isEmpty()) {
            return Result.UNKNOWN;
        }
        for (BigDecimal base : bases) {
            BigDecimal expected = base.multiply(BigDecimal.ONE.add(rate.divide(HUNDRED, MathContext.DECIMAL64)));
            BigDecimal allowed = expected.abs().multiply(properties.tolerance()).add(base.multiply(RATE_ROUNDING));
            if (now.subtract(expected).abs().compareTo(allowed) <= 0) {
                return Result.OK;
            }
        }
        return Result.BAD;
    }

    // 저가 ≤ 현재가 ≤ 고가. 고가·저가는 같은 응답 안의 값이라 방금 갱신된 고가·저가도 현재가를 포함한다.
    private Result highLow(JsonNode body, BigDecimal now) {
        BigDecimal high = number(body, properties.highField());
        BigDecimal low = number(body, properties.lowField());
        if (high == null || low == null) {
            return Result.UNKNOWN;
        }
        BigDecimal margin = BigDecimal.ONE.add(properties.tolerance());
        BigDecimal lowMargin = BigDecimal.ONE.subtract(properties.tolerance());
        boolean inside = now.compareTo(low.multiply(lowMargin)) >= 0 && now.compareTo(high.multiply(margin)) <= 0;
        return inside ? Result.OK : Result.BAD;
    }

    private Integer sign(String code) {
        if (code == null) {
            return null;
        }
        if (properties.signUp().contains(code)) {
            return 1;
        }
        if (properties.signDown().contains(code)) {
            return -1;
        }
        return properties.signFlat().contains(code) ? 0 : null;
    }

    private static String text(JsonNode body, String field) {
        if (PriceCheckProperties.isBlank(field)) {
            return null;
        }
        JsonNode node = body.path(field);
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        String value = node.asString("").trim();
        return value.isEmpty() ? null : value;
    }

    // 숫자 칸은 앞에 공백이 붙어 올 수 있다. 비었거나 숫자가 아니면 그 칸으로는 판단하지 않는다.
    private static BigDecimal number(JsonNode body, String field) {
        String value = text(body, field);
        if (value == null) {
            return null;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

package dev.stockreading.market.context;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.market.event.ContextPayload;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 세계지수 원문의 지수마다 등락률을 계산한다.
 *
 * <p>원문의 전일대비·등락률은 장이 끝난 지수에서 평일에도 0으로 오고 전일 종가 필드가 없어 쓰지 않는다.
 * 대신 같은 대륙의 저장 원문 중 받은 시각이 [since, asOf] 안(since는 판 범위가 아니라 직전 종가를 찾는 하한)이고
 * 같은 지수 코드의 영업일이 더 이른, 가장 최근에 받은 행의 종가를 직전 종가로 삼는다. 지수 코드는 한 대륙에만 속하므로 같은 대륙 행만 본다.
 */
@Component
class WorldIndexRates {

    static final String CODE_FIELD = "is_cd";
    static final String NAME_FIELD = "is_cd_nm";
    static final String DAY_FIELD = "bsnss_dy";
    static final String CLOSE_FIELD = "cls_prc_p2";

    private static final int RATE_SCALE = 2;
    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);

    private static final Logger log = LoggerFactory.getLogger(WorldIndexRates.class);

    private final ObjectMapper mapper;

    WorldIndexRates(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param current 배경 자료로 고른 원문
     * @param rows    같은 대륙의 후보 원문(받은 시각이 늦은 순). current가 섞여 있어도 영업일이 같아 직전 행이 되지 않는다
     * @param since   직전 행을 찾는 받은 시각 하한(포함). 이보다 일찍 받은 행은 너무 오래되어 직전 행으로 쓰지 않는다
     * @param asOf    기준 시각(포함). 이보다 늦게 받은 행은 그 시각에 알 수 없던 자료라 쓰지 않는다
     * @return current의 지수 순서대로 계산값. 코드·영업일·종가를 읽을 수 없는 지수는 뺀다
     */
    List<ContextPayload.IndexRate> compute(String current, List<BackgroundRepository.StoredResponse> rows,
            OffsetDateTime since, OffsetDateTime asOf) {
        List<IndexRecord> earlier = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            BackgroundRepository.StoredResponse row = rows.get(i);
            // 저장소가 기간을 지키더라도 여기서 한 번 더 확인한다. 어기면 조용히 미래·오래된 값으로 계산된다.
            if (row.fetchedAt().isBefore(since) || row.fetchedAt().isAfter(asOf)) {
                continue;
            }
            earlier.addAll(records(row.response(), i));
        }
        List<ContextPayload.IndexRate> rates = new ArrayList<>();
        for (IndexRecord record : records(current, -1)) {
            IndexRecord previous = earlier.stream()
                    .filter(candidate -> candidate.code().equals(record.code())
                            && candidate.day().isBefore(record.day()))
                    .findFirst()
                    .orElse(null);
            rates.add(new ContextPayload.IndexRate(
                    record.code(), record.name(), record.dayText(), record.closeText(),
                    previous == null ? null : previous.dayText(),
                    previous == null ? null : previous.closeText(),
                    previous == null ? null : rate(record.close(), previous.close())));
        }
        return rates;
    }

    private static String rate(BigDecimal close, BigDecimal previousClose) {
        if (previousClose.signum() == 0) {
            return null;
        }
        return close.subtract(previousClose)
                .multiply(HUNDRED)
                .divide(previousClose, RATE_SCALE, RoundingMode.HALF_UP)
                .toPlainString();
    }

    // rows는 받은 시각이 늦은 순이고 한 원문 안의 순서도 유지하므로, 앞에 나온 레코드가 더 최근에 받은 것이다.
    private List<IndexRecord> records(String response, int rowIndex) {
        JsonNode items;
        try {
            items = mapper.readTree(response).path("dataBody").path("out2");
        } catch (JacksonException e) {
            log.warn("world index row skipped: response is not JSON row={}", rowIndex);
            return List.of();
        }
        List<IndexRecord> records = new ArrayList<>();
        for (JsonNode item : items) {
            IndexRecord record = record(item);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    private static IndexRecord record(JsonNode item) {
        String code = text(item, CODE_FIELD);
        String day = text(item, DAY_FIELD);
        String close = text(item, CLOSE_FIELD);
        if (code == null || day == null || close == null) {
            return null;
        }
        try {
            String name = text(item, NAME_FIELD);
            return new IndexRecord(code, name == null ? "" : name, day,
                    LocalDate.parse(day, DateTimeFormatter.BASIC_ISO_DATE), close, new BigDecimal(close));
        } catch (DateTimeParseException | NumberFormatException e) {
            return null;
        }
    }

    private static String text(JsonNode item, String field) {
        JsonNode value = item.path(field);
        if (!value.isString() || value.asString().isBlank()) {
            return null;
        }
        return value.asString().strip();
    }

    private record IndexRecord(
            String code, String name, String dayText, LocalDate day, String closeText, BigDecimal close) {
    }
}

package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.market.MarketProperties;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** 스냅샷 하나가 유형 1(window), 유형 2(prevClose)에 해당하는지 평가한다. 변동률은 코드가 계산한다(ADR-007). */
@Component
class MoveDetector {

    private static final Logger log = LoggerFactory.getLogger(MoveDetector.class);

    private final PriceSnapshotRepository repository;
    private final MarketProperties properties;
    private final ObjectMapper mapper;

    MoveDetector(PriceSnapshotRepository repository, MarketProperties properties, ObjectMapper mapper) {
        this.repository = repository;
        this.properties = properties;
        this.mapper = mapper;
        if (!prevCloseEnabled()) {
            log.warn("prevClose disabled: MARKET_PREV_CLOSE_FIELDS is empty");
        }
    }

    /** 칸 목록을 비우면 스냅샷마다 로그를 남기지 않고 유형 2를 끈다. 테스트용 발송기처럼 유형 2가 방해되는 곳에서 쓴다. */
    private boolean prevCloseEnabled() {
        List<String> fields = properties.prevCloseFields();
        return fields != null && !fields.isEmpty();
    }

    List<Evaluation> evaluate(String symbol, OffsetDateTime asOf, long price, String raw) {
        return List.of(evaluateWindow(symbol, asOf, price), evaluatePrevClose(symbol, asOf, price, raw));
    }

    /**
     * 최근 N분 이력의 가장 오래된 스냅샷을 기준으로 삼는다. 변동 시각은 기준 이후 스냅샷 중
     * 기준 대비 변동률이 임계값 미만이던 가장 최근 것의 시각이고, 그런 스냅샷이 없으면 기준의 시각이다.
     */
    private Evaluation evaluateWindow(String symbol, OffsetDateTime asOf, long price) {
        BigDecimal threshold = properties.windowThresholdPercent();
        List<PricePoint> history = repository.findBetween(symbol, asOf.minusMinutes(properties.windowMinutes()), asOf);
        if (history.isEmpty()) {
            log.info("{} window skipped: no snapshot in the last {} minutes (first snapshot or collection gap)",
                    symbol, properties.windowMinutes());
            return Evaluation.clear(DetectionType.WINDOW);
        }
        PricePoint base = history.get(0);
        if (base.price() <= 0) {
            log.info("{} window skipped: baseline price is not positive", symbol);
            return Evaluation.unknown(DetectionType.WINDOW);
        }
        BigDecimal change = ChangeRate.percent(base.price(), price);
        if (change.abs().compareTo(threshold) < 0) {
            return Evaluation.clear(DetectionType.WINDOW);
        }
        OffsetDateTime startedAt = base.asOf();
        for (int i = history.size() - 1; i >= 1; i--) {
            PricePoint point = history.get(i);
            if (ChangeRate.percent(base.price(), point.price()).abs().compareTo(threshold) < 0) {
                startedAt = point.asOf();
                break;
            }
        }
        return Evaluation.detected(new Detection(DetectionType.WINDOW, base.price(), price, change, threshold, startedAt));
    }

    /** 전일 종가는 원문의 여러 칸을 다수결로 정한다. 다수가 없으면 이 스냅샷은 판단하지 않는다. */
    private Evaluation evaluatePrevClose(String symbol, OffsetDateTime asOf, long price, String raw) {
        if (!prevCloseEnabled()) {
            return Evaluation.unknown(DetectionType.PREV_CLOSE);
        }
        BigDecimal threshold = properties.prevCloseThresholdPercent();
        Long prevClose = readPrevClose(symbol, raw);
        if (prevClose == null) {
            return Evaluation.unknown(DetectionType.PREV_CLOSE);
        }
        BigDecimal change = ChangeRate.percent(prevClose, price);
        if (change.abs().compareTo(threshold) < 0) {
            return Evaluation.clear(DetectionType.PREV_CLOSE);
        }
        // 처음 넘은 시각은 이 스냅샷이다. 이미 발행한 변동이면 MoveTracker가 저장된 시각으로 바꾼다.
        return Evaluation.detected(new Detection(DetectionType.PREV_CLOSE, prevClose, price, change, threshold, asOf));
    }

    /**
     * 제공자가 전일 종가 칸을 하루 안에서도 깨뜨려 보내서(2026-11-04 확인) 한 칸만 믿을 수 없다.
     * 읽을 수 있는 값 중 설정한 칸의 절반을 넘는 칸이 같은 값일 때만 그 값을 쓴다.
     */
    private Long readPrevClose(String symbol, String raw) {
        if (raw == null || raw.isBlank()) {
            log.info("{} prevClose skipped: snapshot has no raw response", symbol);
            return null;
        }
        JsonNode root;
        try {
            root = mapper.readTree(raw);
        } catch (JacksonException e) {
            log.info("{} prevClose skipped: raw response is not JSON", symbol);
            return null;
        }
        List<String> fields = properties.prevCloseFields();
        Map<Long, Integer> votes = new HashMap<>();
        for (String field : fields) {
            Long value = readField(root, field);
            if (value != null) {
                votes.merge(value, 1, Integer::sum);
            }
        }
        List<Long> majority = new ArrayList<>();
        votes.forEach((value, count) -> {
            if (count * 2 > fields.size()) {
                majority.add(value);
            }
        });
        if (majority.size() != 1) {
            log.info("{} prevClose skipped: no majority among {} fields", symbol, fields.size());
            return null;
        }
        return majority.get(0);
    }

    static Long readField(JsonNode root, String field) {
        JsonNode node = root;
        for (String name : field.split("\\.")) {
            node = node.path(name);
        }
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        BigDecimal value;
        try {
            value = new BigDecimal(node.asString().trim());
        } catch (NumberFormatException e) {
            return null;
        }
        if (value.signum() <= 0 || value.stripTrailingZeros().scale() > 0) {
            return null;
        }
        try {
            return value.longValueExact();
        } catch (ArithmeticException e) {
            return null;
        }
    }
}

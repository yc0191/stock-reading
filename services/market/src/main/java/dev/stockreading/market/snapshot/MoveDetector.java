package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

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
    }

    List<Evaluation> evaluate(String symbol, OffsetDateTime asOf, long price, String raw) {
        return List.of(evaluateWindow(symbol, asOf, price), evaluatePrevClose(symbol, asOf, price, raw));
    }

    /**
     * 최근 N분 이력의 가장 오래된 스냅샷을 기준으로 삼는다. 변동 시각은 기준 이후 스냅샷 중
     * 기준 대비 변동률이 임계값 미만이던 가장 최근 것의 시각이고, 그런 스냅샷이 없으면 기준의 시각이다(ADR-019).
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

    /** 전일 종가는 원문에서 읽는다. 필드가 없거나 양의 정수가 아니면 판단하지 않는다. */
    private Evaluation evaluatePrevClose(String symbol, OffsetDateTime asOf, long price, String raw) {
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

    private Long readPrevClose(String symbol, String raw) {
        String field = properties.prevCloseField();
        if (raw == null || raw.isBlank()) {
            log.info("{} prevClose skipped: snapshot has no raw response", symbol);
            return null;
        }
        JsonNode node;
        try {
            node = mapper.readTree(raw);
        } catch (JacksonException e) {
            log.info("{} prevClose skipped: raw response is not JSON", symbol);
            return null;
        }
        for (String name : field.split("\\.")) {
            node = node.path(name);
        }
        if (node.isMissingNode() || node.isNull()) {
            log.info("{} prevClose skipped: field '{}' not found in raw response", symbol, field);
            return null;
        }
        BigDecimal value;
        try {
            value = new BigDecimal(node.asString().trim());
        } catch (NumberFormatException e) {
            log.info("{} prevClose skipped: field '{}' is not a number", symbol, field);
            return null;
        }
        if (value.signum() <= 0 || value.stripTrailingZeros().scale() > 0) {
            log.info("{} prevClose skipped: field '{}' is not a positive integer", symbol, field);
            return null;
        }
        return value.longValueExact();
    }
}

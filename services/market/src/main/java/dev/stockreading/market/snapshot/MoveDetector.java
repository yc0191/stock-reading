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
        if (!prevCloseEnabled()) {
            log.warn("prevClose disabled: MARKET_PREV_CLOSE_FIELDS is empty");
        }
    }

    /** 칸 목록을 비우면 스냅샷마다 로그를 남기지 않고 유형 2를 끈다. 테스트용 발송기처럼 유형 2가 방해되는 곳에서 쓴다. */
    private boolean prevCloseEnabled() {
        List<String> fields = properties.prevCloseFields();
        return fields != null && !fields.isEmpty();
    }

    /**
     * 전일 종가는 한 번만 읽어 두 유형이 함께 쓴다. 유형 1도 오늘 등락률(전일 종가 대비)을 담아, 받는 쪽이 짧은 변동률이 아니라
     * 같은 하루 기준으로 시장과 비교하게 한다.
     */
    List<Evaluation> evaluate(String symbol, OffsetDateTime asOf, long price, String raw) {
        Long prevClose = prevCloseEnabled() ? readPrevClose(symbol, raw) : null;
        BigDecimal today = prevClose == null ? null : ChangeRate.percent(prevClose, price);
        return List.of(evaluateWindow(symbol, asOf, price, today), evaluatePrevClose(asOf, price, prevClose, today));
    }

    /**
     * 최근 N분 이력에서 첫 체결 전 응답과 연속 확인을 받지 못한 튄 점을 뺀 가장 오래된 스냅샷을 기준으로 삼는다.
     * 변동 시각은 기준 이후 스냅샷 중 기준 대비 변동률이 임계값 미만이던 가장 최근 것의 시각이고,
     * 그런 스냅샷이 없으면 기준의 시각이다.
     */
    private Evaluation evaluateWindow(String symbol, OffsetDateTime asOf, long price, BigDecimal today) {
        BigDecimal threshold = properties.windowThresholdPercent();
        OffsetDateTime windowStart = asOf.minusMinutes(properties.windowMinutes());
        List<PricePoint> history = baseline(symbol, windowStart, repository.findBetween(symbol, windowStart, asOf, firstTradeFields()));
        if (history.isEmpty()) {
            log.info("{} window skipped: no usable snapshot in the last {} minutes (first snapshot, collection gap or before the first trade)",
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
        return Evaluation.detected(new Detection(DetectionType.WINDOW, base.price(), price, change, threshold, startedAt, today));
    }

    private List<String> firstTradeFields() {
        List<String> fields = properties.firstTradeFields();
        return fields == null ? List.of() : fields;
    }

    /**
     * 기준값으로 쓸 이력을 고른다. 첫 체결 전 응답은 현재가가 전일 종가 그대로라 빼고, 맨 앞의 튄 점도 뺀다.
     * 튄 점은 맨 앞 값과 임계값 미만으로 비슷한 연속 구간이 앞뒤를 합쳐 confirmCount건보다 짧고, 앞뒤 값이 모두 다른 경우다.
     * 같은 건수로 확인받지 못한 변동은 발행하지 않으므로 기준으로도 쓰지 않는다. 튄 점이 기준이 되면 5분 뒤에
     * 제자리로 돌아온 값이 반대 방향 변동으로 발행된다. 앞 값이 없으면(수집이 처음 시작된 곳) 튄 점인지 알 수 없어 그대로 쓴다.
     */
    private List<PricePoint> baseline(String symbol, OffsetDateTime windowStart, List<PricePoint> history) {
        List<PricePoint> traded = history.stream().filter(point -> !point.beforeFirstTrade()).toList();
        BigDecimal threshold = properties.windowThresholdPercent();
        List<PricePoint> prior = null;
        int start = 0;
        while (start < traded.size() && traded.get(start).price() > 0) {
            long basePrice = traded.get(start).price();
            int end = start + 1;
            while (end < traded.size() && similar(basePrice, traded.get(end).price(), threshold)) {
                end++;
            }
            int run = end - start;
            if (end == traded.size() || run >= properties.confirmCount()) {
                break;
            }
            if (start == 0) {
                // 창 앞에 이어지던 값이 있는지 본다. 꼭 필요할 때만 조회한다.
                if (prior == null) {
                    prior = repository.findLatestBetween(symbol, windowStart.minusMinutes(properties.windowMinutes()),
                            windowStart, properties.confirmCount() - 1, firstTradeFields()).stream()
                            .filter(point -> !point.beforeFirstTrade()).toList();
                }
                if (prior.isEmpty()) {
                    break;
                }
                for (int i = prior.size() - 1; i >= 0 && similar(basePrice, prior.get(i).price(), threshold); i--) {
                    run++;
                }
                if (run >= properties.confirmCount()) {
                    break;
                }
            }
            start = end;
        }
        return traded.subList(start, traded.size());
    }

    private static boolean similar(long basePrice, long price, BigDecimal threshold) {
        return ChangeRate.percent(basePrice, price).abs().compareTo(threshold) < 0;
    }

    /** 전일 종가는 원문의 여러 칸을 다수결로 정한다. 다수가 없으면 이 스냅샷은 판단하지 않는다. */
    private Evaluation evaluatePrevClose(OffsetDateTime asOf, long price, Long prevClose, BigDecimal change) {
        if (prevClose == null) {
            return Evaluation.unknown(DetectionType.PREV_CLOSE);
        }
        BigDecimal threshold = properties.prevCloseThresholdPercent();
        if (change.abs().compareTo(threshold) < 0) {
            return Evaluation.clear(DetectionType.PREV_CLOSE);
        }
        // 처음 넘은 시각은 이 스냅샷이다. 이미 발행한 변동이면 MoveTracker가 저장된 시각으로 바꾼다.
        return Evaluation.detected(
                new Detection(DetectionType.PREV_CLOSE, prevClose, price, change, threshold, asOf, change));
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
        Long majority = RawQuote.majority(root, properties.prevCloseFields());
        if (majority == null) {
            log.info("{} prevClose skipped: no majority among {} fields", symbol, properties.prevCloseFields().size());
        }
        return majority;
    }

    static Long readField(JsonNode root, String field) {
        return RawQuote.readField(root, field);
    }
}

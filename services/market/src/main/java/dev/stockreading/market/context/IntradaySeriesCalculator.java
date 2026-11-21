package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.snapshot.ChangeRate;
import dev.stockreading.market.snapshot.RawQuote;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * 그래프용 장중 흐름을 저장한 시세로 만든다. 1분마다 그 분의 마지막 시세 한 점을 쓰고, 등락률은 응답 값을 믿지 않고
 * 구간 마지막 스냅샷 원문의 전일 종가(후보 칸 과반, 개장 흐름과 같은 규칙)와 가격으로 코드가 계산한다(ADR-007).
 */
class IntradaySeriesCalculator {

    private final IntradaySeriesPort series;
    private final OpenFlowPort snapshots;
    private final ObjectMapper mapper;
    private final List<String> prevCloseFields;

    IntradaySeriesCalculator(IntradaySeriesPort series, OpenFlowPort snapshots, ObjectMapper mapper,
            List<String> prevCloseFields) {
        this.series = series;
        this.snapshots = snapshots;
        this.mapper = mapper;
        this.prevCloseFields = prevCloseFields == null ? List.of() : prevCloseFields;
    }

    DailyPayload.IntradaySeries calculate(String symbol, OffsetDateTime since, OffsetDateTime asOf, int maxPoints) {
        // 한 건 더 읽어 상한을 넘었는지 안다.
        List<IntradaySeriesPort.Point> newestFirst = series.minuteLasts(symbol, since, asOf, maxPoints + 1);
        if (newestFirst.isEmpty()) {
            return DailyPayload.IntradaySeries.none();
        }
        boolean truncated = newestFirst.size() > maxPoints;
        Long prevClose = prevClose(symbol, since, asOf);
        List<DailyPayload.IntradayPoint> points = new ArrayList<>();
        for (IntradaySeriesPort.Point point : newestFirst.subList(0, Math.min(maxPoints, newestFirst.size())).reversed()) {
            // 저장소가 기간을 지키더라도 여기서 한 번 더 확인한다. 어기면 판 시각 뒤 시세가 조용히 그래프에 들어간다.
            if (point.asOf().isBefore(since) || point.asOf().isAfter(asOf) || point.price() <= 0) {
                continue;
            }
            // 가격제한폭(30%)을 넘게 전일 종가와 다르면 같은 날 가격일 수 없는 깨진 시세라 점에서 뺀다.
            if (prevClose != null && OpenFlowCalculator.differsTooMuch(point.price(), prevClose)) {
                continue;
            }
            points.add(new DailyPayload.IntradayPoint(EventFactory.formatKst(point.asOf()),
                    Long.toString(point.price()),
                    prevClose == null ? null : ChangeRate.percent(prevClose, point.price()).toPlainString()));
        }
        if (points.isEmpty()) {
            return DailyPayload.IntradaySeries.none();
        }
        return new DailyPayload.IntradaySeries(DailyPayload.INTRADAY_SERIES_PRESENT,
                prevClose == null ? null : Long.toString(prevClose), truncated, List.copyOf(points));
    }

    /** 구간 마지막 스냅샷 원문의 전일 종가. 읽지 못하거나 0 이하이면 null(등락률을 계산하지 않는다). */
    private Long prevClose(String symbol, OffsetDateTime since, OffsetDateTime asOf) {
        if (prevCloseFields.isEmpty()) {
            return null;
        }
        Optional<OpenFlowPort.Snapshot> last = snapshots.last(symbol, since, asOf);
        JsonNode raw = last.map(s -> parse(s.raw())).orElse(null);
        Long value = raw == null ? null : RawQuote.majority(raw, prevCloseFields);
        return value == null || value <= 0 ? null : value;
    }

    private JsonNode parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return mapper.readTree(raw);
        } catch (JacksonException e) {
            return null;
        }
    }
}

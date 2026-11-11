package dev.stockreading.market.context;

import java.time.OffsetDateTime;
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
 * 저장한 시세 두 건(구간의 첫 스냅샷, 마지막 스냅샷)으로 ETF 하나의 개장 흐름을 계산한다. 비율은 응답 값을 믿지 않고
 * 전일 종가·시가·현재가로 코드가 계산한다(ADR-007).
 */
class OpenFlowCalculator {

    static final long MAX_OPEN_GAP_PERCENT = 30;

    private final OpenFlowPort port;
    private final ObjectMapper mapper;
    private final List<String> openPriceFields;
    private final List<String> prevCloseFields;

    OpenFlowCalculator(OpenFlowPort port, ObjectMapper mapper, List<String> openPriceFields,
            List<String> prevCloseFields) {
        this.port = port;
        this.mapper = mapper;
        this.openPriceFields = openPriceFields == null ? List.of() : openPriceFields;
        this.prevCloseFields = prevCloseFields == null ? List.of() : prevCloseFields;
    }

    DailyPayload.OpenFlow calculate(String symbol, OffsetDateTime since, OffsetDateTime asOf) {
        Optional<OpenFlowPort.Snapshot> first = port.first(symbol, since, asOf);
        Optional<OpenFlowPort.Snapshot> last = port.last(symbol, since, asOf);
        if (first.isEmpty() || last.isEmpty()) {
            return DailyPayload.OpenFlow.none();
        }
        OpenFlowPort.Snapshot start = first.get();
        OpenFlowPort.Snapshot end = last.get();
        JsonNode raw = parse(end.raw());
        Long prevClose = raw == null || prevCloseFields.isEmpty() ? null : RawQuote.majority(raw, prevCloseFields);
        Long open = raw == null ? null : firstReadable(raw);
        if (open != null && start.price() > 0 && differsTooMuch(open, start.price())) {
            // 시가 칸이 깨진 값이면 첫 시세 가격으로 대신한다.
            open = null;
        }
        String source = open == null ? DailyPayload.OPEN_SOURCE_FIRST_SNAPSHOT : DailyPayload.OPEN_SOURCE_RAW;
        if (open == null) {
            open = start.price() > 0 ? start.price() : null;
        }
        if (open == null) {
            source = null;
        }
        return new DailyPayload.OpenFlow(
                DailyPayload.OPEN_FLOW_PRESENT,
                text(prevClose),
                text(open),
                source,
                prevClose != null && open != null ? percent(prevClose, open) : null,
                EventFactory.formatKst(start.asOf()),
                Long.toString(start.price()),
                EventFactory.formatKst(end.asOf()),
                Long.toString(end.price()),
                start.price() > 0 ? percent(start.price(), end.price()) : null,
                prevClose != null ? percent(prevClose, end.price()) : null);
    }

    // 가격제한폭(30%)을 넘게 다르면 같은 날 가격으로 볼 수 없다. 시가는 장 시작 직후 첫 시세와 가까워야 한다.
    private static boolean differsTooMuch(long open, long first) {
        return Math.abs(open - first) * 100 > first * MAX_OPEN_GAP_PERCENT;
    }

    private Long firstReadable(JsonNode raw) {
        for (String field : openPriceFields) {
            Long value = RawQuote.readField(raw, field);
            if (value != null) {
                return value;
            }
        }
        return null;
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

    private static String text(Long value) {
        return value == null ? null : Long.toString(value);
    }

    private static String percent(long previous, long current) {
        return ChangeRate.percent(previous, current).toPlainString();
    }
}

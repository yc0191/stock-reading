package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.stockreading.market.event.DailyPayload;
import tools.jackson.databind.json.JsonMapper;

/** 저장한 시세 두 건으로 개장 흐름을 계산하는 규칙. 종목코드와 가격은 모두 가짜다. */
class OpenFlowCalculatorTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-11-12T09:00:00+09:00");
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-11-12T09:10:00+09:00");

    private OpenFlowPort port;
    private OpenFlowCalculator calculator;

    @BeforeEach
    void setUp() {
        port = mock(OpenFlowPort.class);
        calculator = new OpenFlowCalculator(port, JsonMapper.builder().build(), List.of("f.open"),
                List.of("f.a", "f.b", "f.c"));
    }

    private static OpenFlowPort.Snapshot snapshot(String time, long price, String raw) {
        return new OpenFlowPort.Snapshot(OffsetDateTime.parse("2026-11-12T" + time + "+09:00"), price, raw);
    }

    private static String raw(String open, String a, String b, String c) {
        return "{\"f\":{\"open\":" + open + ",\"a\":" + a + ",\"b\":" + b + ",\"c\":" + c + "}}";
    }

    private void snapshots(OpenFlowPort.Snapshot first, OpenFlowPort.Snapshot last) {
        when(port.first(anyString(), any(), any())).thenReturn(Optional.ofNullable(first));
        when(port.last(anyString(), any(), any())).thenReturn(Optional.ofNullable(last));
    }

    @Test
    void computesRatesFromPricesAndNotFromTheResponseValues() {
        snapshots(snapshot("09:00:01", 10_100, null),
                snapshot("09:09:58", 10_300, raw("10200", "10000", "10000", "10000")));

        DailyPayload.OpenFlow flow = calculator.calculate("TEST01", SINCE, AS_OF);

        assertThat(flow.status()).isEqualTo("present");
        assertThat(flow.prevClose()).isEqualTo("10000");
        assertThat(flow.open()).isEqualTo("10200");
        assertThat(flow.openSource()).isEqualTo("raw");
        assertThat(flow.openRate()).isEqualTo("2.00");
        assertThat(flow.startAt()).isEqualTo("2026-11-12T09:00:01+09:00");
        assertThat(flow.startPrice()).isEqualTo("10100");
        assertThat(flow.endAt()).isEqualTo("2026-11-12T09:09:58+09:00");
        assertThat(flow.endPrice()).isEqualTo("10300");
        assertThat(flow.flowRate()).isEqualTo("1.98");
        assertThat(flow.rateVsPrevClose()).isEqualTo("3.00");
    }

    @Test
    void usesTheFirstSnapshotPriceWhenTheOpenCellIsEmptyNotANumberOrZero() {
        for (String open : List.of("\"\"", "\"abc\"", "0", "null")) {
            snapshots(snapshot("09:00:01", 10_100, null),
                    snapshot("09:09:58", 10_300, raw(open, "10000", "10000", "10000")));

            DailyPayload.OpenFlow flow = calculator.calculate("TEST01", SINCE, AS_OF);

            assertThat(flow.open()).as(open).isEqualTo("10100");
            assertThat(flow.openSource()).as(open).isEqualTo("first-snapshot");
            assertThat(flow.openRate()).as(open).isEqualTo("1.00");
        }
    }

    @Test
    void dropsAnOpenThatDiffersFromTheFirstSnapshotByMoreThanThirtyPercent() {
        // 시가 칸이 수량 등 다른 값을 담은 경우(깨진 값)는 +9900% 같은 등락률이 나가지 않게 첫 시세 가격으로 대신한다.
        for (String open : List.of("1020000", "13001", "6999")) {
            snapshots(snapshot("09:00:01", 10_000, null),
                    snapshot("09:09:58", 10_300, raw(open, "10000", "10000", "10000")));

            DailyPayload.OpenFlow flow = calculator.calculate("TEST01", SINCE, AS_OF);

            assertThat(flow.open()).as(open).isEqualTo("10000");
            assertThat(flow.openSource()).as(open).isEqualTo("first-snapshot");
            assertThat(flow.openRate()).as(open).isEqualTo("0.00");
        }
    }

    @Test
    void keepsAnOpenExactlyThirtyPercentAwayFromTheFirstSnapshot() {
        for (String open : List.of("13000", "7000")) {
            snapshots(snapshot("09:00:01", 10_000, null),
                    snapshot("09:09:58", 10_300, raw(open, "10000", "10000", "10000")));

            DailyPayload.OpenFlow flow = calculator.calculate("TEST01", SINCE, AS_OF);

            assertThat(flow.open()).as(open).isEqualTo(open);
            assertThat(flow.openSource()).as(open).isEqualTo("raw");
        }
    }

    @Test
    void leavesPrevCloseAndItsRatesEmptyWhenNoMajorityAgrees() {
        snapshots(snapshot("09:00:01", 10_100, null),
                snapshot("09:09:58", 10_300, raw("10200", "10000", "9000", "8000")));

        DailyPayload.OpenFlow flow = calculator.calculate("TEST01", SINCE, AS_OF);

        assertThat(flow.prevClose()).isNull();
        assertThat(flow.openRate()).isNull();
        assertThat(flow.rateVsPrevClose()).isNull();
        assertThat(flow.flowRate()).isEqualTo("1.98");
    }

    @Test
    void stillAnswersWhenTheLastSnapshotHasNoReadableRaw() {
        snapshots(snapshot("09:00:01", 10_100, null), snapshot("09:09:58", 10_300, "not json"));

        DailyPayload.OpenFlow flow = calculator.calculate("TEST01", SINCE, AS_OF);

        assertThat(flow.status()).isEqualTo("present");
        assertThat(flow.open()).isEqualTo("10100");
        assertThat(flow.openSource()).isEqualTo("first-snapshot");
        assertThat(flow.prevClose()).isNull();
    }

    @Test
    void reportsNoSnapshotWhenTheRangeHasNoQuote() {
        snapshots(null, null);

        DailyPayload.OpenFlow flow = calculator.calculate("TEST01", SINCE, AS_OF);

        assertThat(flow).isEqualTo(DailyPayload.OpenFlow.none());
        assertThat(flow.status()).isEqualTo("no-snapshot");
        assertThat(flow.open()).isNull();
    }

    @Test
    void aSingleSnapshotGivesAZeroFlow() {
        OpenFlowPort.Snapshot only = snapshot("09:05:00", 10_100, raw("10050", "10000", "10000", "10000"));
        snapshots(only, only);

        assertThat(calculator.calculate("TEST01", SINCE, AS_OF).flowRate()).isEqualTo("0.00");
    }
}

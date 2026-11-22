package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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

/** 1분 간격으로 솎은 시세를 그래프용 장중 흐름으로 바꾸는 규칙. 값은 모두 가짜다. */
class IntradaySeriesCalculatorTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-20T09:00:00+09:00");
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-20T17:00:00+09:00");
    private static final String RAW = "{\"f\":{\"a\":10000,\"b\":10000,\"c\":10000}}";

    private IntradaySeriesPort series;
    private OpenFlowPort snapshots;
    private IntradaySeriesCalculator calculator;

    @BeforeEach
    void setUp() {
        series = mock(IntradaySeriesPort.class);
        snapshots = mock(OpenFlowPort.class);
        calculator = new IntradaySeriesCalculator(series, snapshots, JsonMapper.builder().build(),
                List.of("f.a", "f.b", "f.c"));
        when(snapshots.last(anyString(), any(), any()))
                .thenReturn(Optional.of(new OpenFlowPort.Snapshot(AS_OF.minusHours(2), 9_900, RAW)));
    }

    private static IntradaySeriesPort.Point point(String time, long price) {
        return new IntradaySeriesPort.Point(OffsetDateTime.parse("2026-10-20T" + time + "+09:00"), price);
    }

    private void newestFirst(IntradaySeriesPort.Point... points) {
        when(series.minuteLasts(anyString(), any(), any(), anyInt())).thenReturn(List.of(points));
    }

    @Test
    void pointsComeOutOldestFirstWithRatesAgainstThePrevCloseOfTheLastSnapshot() {
        newestFirst(point("15:30:00", 9_900), point("09:01:58", 10_050), point("09:00:59", 10_000));

        DailyPayload.IntradaySeries result = calculator.calculate("TEST01", SINCE, AS_OF, 420);

        assertThat(result.status()).isEqualTo(DailyPayload.INTRADAY_SERIES_PRESENT);
        assertThat(result.prevClose()).isEqualTo("10000");
        assertThat(result.truncated()).isFalse();
        assertThat(result.points()).extracting(DailyPayload.IntradayPoint::at, DailyPayload.IntradayPoint::price,
                        DailyPayload.IntradayPoint::rateVsPrevClose)
                .containsExactly(tuple("2026-10-20T09:00:59+09:00", "10000", "0.00"),
                        tuple("2026-10-20T09:01:58+09:00", "10050", "0.50"),
                        tuple("2026-10-20T15:30:00+09:00", "9900", "-1.00"));
    }

    @Test
    void moreMinutesThanTheLimitKeepTheLatestOnesAndAreMarkedTruncated() {
        newestFirst(point("09:02:00", 10_200), point("09:01:00", 10_100), point("09:00:00", 10_000));

        DailyPayload.IntradaySeries result = calculator.calculate("TEST01", SINCE, AS_OF, 2);

        assertThat(result.truncated()).isTrue();
        assertThat(result.points()).extracting(DailyPayload.IntradayPoint::price).containsExactly("10100", "10200");
    }

    @Test
    void noQuotesGiveANoSnapshotSeries() {
        newestFirst();

        assertThat(calculator.calculate("TEST01", SINCE, AS_OF, 420)).isEqualTo(DailyPayload.IntradaySeries.none());
    }

    @Test
    void anUnreadablePrevCloseKeepsThePricesButLeavesTheRatesEmpty() {
        newestFirst(point("09:00:59", 10_000));
        when(snapshots.last(anyString(), any(), any()))
                .thenReturn(Optional.of(new OpenFlowPort.Snapshot(AS_OF, 10_000, "{\"f\":{\"a\":10000,\"b\":9000}}")));

        DailyPayload.IntradaySeries result = calculator.calculate("TEST01", SINCE, AS_OF, 420);

        assertThat(result.prevClose()).isNull();
        assertThat(result.points()).extracting(DailyPayload.IntradayPoint::price, DailyPayload.IntradayPoint::rateVsPrevClose)
                .containsExactly(tuple("10000", null));
    }

    @Test
    void aBrokenQuoteBeyondThePriceLimitAndAPointOutsideTheRangeAreLeftOut() {
        // 전일 종가 10000 대비 30%를 넘는 가격, 판 시각 뒤 시세는 그래프에 넣지 않는다.
        newestFirst(point("17:00:01", 10_000), point("09:02:00", 13_001), point("09:01:00", 13_000),
                point("09:00:00", 10_000));

        DailyPayload.IntradaySeries result = calculator.calculate("TEST01", SINCE, AS_OF, 420);

        assertThat(result.points()).extracting(DailyPayload.IntradayPoint::price).containsExactly("10000", "13000");
    }

    @Test
    void onlyBrokenQuotesGiveANoSnapshotSeries() {
        newestFirst(point("09:00:00", 1));

        assertThat(calculator.calculate("TEST01", SINCE, AS_OF, 420).status())
                .isEqualTo(DailyPayload.INTRADAY_SERIES_NO_SNAPSHOT);
    }
}

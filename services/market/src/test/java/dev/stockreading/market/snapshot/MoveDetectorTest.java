package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.stockreading.market.MarketProperties;
import dev.stockreading.market.snapshot.Evaluation.Outcome;
import tools.jackson.databind.json.JsonMapper;

class MoveDetectorTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-10-20T16:05:00+09:00");

    private PriceSnapshotRepository repository;
    private MoveDetector detector;

    @BeforeEach
    void setUp() {
        repository = mock(PriceSnapshotRepository.class);
        // 가짜 필드 이름이다. 실제 응답 필드는 사용자가 원문을 보고 정한다.
        MarketProperties properties = new MarketProperties(
                5, new BigDecimal("1.00"), "fake.prevClose", new BigDecimal("3.00"));
        detector = new MoveDetector(repository, properties, JsonMapper.builder().build());
    }

    private static PricePoint point(String time, long price) {
        return new PricePoint(OffsetDateTime.parse("2026-10-20T" + time + "+09:00"), price);
    }

    private Evaluation window(long price) {
        return detector.evaluate("TEST01", NOW, price, null).get(0);
    }

    private Evaluation prevClose(long price, String raw) {
        return detector.evaluate("TEST01", NOW, price, raw).get(1);
    }

    private static String raw(String prevClose) {
        return "{\"fake\":{\"prevClose\":" + prevClose + "},\"price\":\"1\"}";
    }

    @Test
    void windowQueryAlwaysHasBothBoundsOfTheConfiguredLength() {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of());

        window(10_000);

        verify(repository).findBetween("TEST01", NOW.minusMinutes(5), NOW);
    }

    @Test
    void windowWithoutHistoryIsNotDetectedAndEndsAnyRunningMove() {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of());

        assertThat(window(10_000).outcome()).isEqualTo(Outcome.CLEAR);
    }

    @ParameterizedTest(name = "price {0} vs base 10000 -> detected={1}")
    @org.junit.jupiter.params.provider.CsvSource({
            "10099, false",
            "10100, true",
            "9901,  false",
            "9900,  true",
            "10500, true"
    })
    void windowComparesAgainstTheOldestSnapshotAtTheThreshold(long price, boolean detected) {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of(point("16:00:00", 10_000)));

        assertThat(window(price).outcome()).isEqualTo(detected ? Outcome.DETECTED : Outcome.CLEAR);
    }

    @Test
    void windowDetectionCarriesBasePriceChangeAndThreshold() {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of(point("16:00:00", 10_000)));

        Detection detection = window(9_850).detection();

        assertThat(detection.type()).isEqualTo(DetectionType.WINDOW);
        assertThat(detection.previousPrice()).isEqualTo(10_000L);
        assertThat(detection.price()).isEqualTo(9_850L);
        assertThat(detection.changePercent()).isEqualByComparingTo("-1.50");
        assertThat(detection.thresholdPercent()).isEqualByComparingTo("1.00");
    }

    @Test
    void windowMoveStartsAtTheLatestSnapshotBelowTheThreshold() {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of(
                point("16:00:00", 10_000),
                point("16:01:00", 10_050),
                point("16:02:00", 10_090),   // 0.90% 미만, 가장 최근의 임계값 미만
                point("16:03:00", 10_120),
                point("16:04:00", 10_150)));

        Evaluation evaluation = window(10_200);

        assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
        assertThat(evaluation.detection().moveStartedAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T16:02:00+09:00"));
    }

    @Test
    void windowMoveStartFallsBackToTheOldestSnapshotWhenNoLaterSnapshotIsBelowTheThreshold() {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of(
                point("16:00:00", 10_000),
                point("16:01:00", 10_150),
                point("16:02:00", 10_200)));

        assertThat(window(10_250).detection().moveStartedAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T16:00:00+09:00"));
    }

    @Test
    void windowMoveStartIsTheOldestSnapshotWhenItIsTheOnlyOne() {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of(point("16:00:00", 10_000)));

        assertThat(window(10_200).detection().moveStartedAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T16:00:00+09:00"));
    }

    @Test
    void windowWithNonPositiveBaselineCannotBeJudged() {
        when(repository.findBetween(anyString(), any(), any())).thenReturn(List.of(point("16:00:00", 0)));

        assertThat(window(10_000).outcome()).isEqualTo(Outcome.UNKNOWN);
    }

    @ParameterizedTest(name = "price {0} vs prevClose 10000 -> detected={1}")
    @org.junit.jupiter.params.provider.CsvSource({
            "10299, false",
            "10300, true",
            "9701,  false",
            "9700,  true",
            "10000, false"
    })
    void prevCloseComparesAgainstTheRawFieldAtTheThreshold(long price, boolean detected) {
        assertThat(prevClose(price, raw("10000")).outcome()).isEqualTo(detected ? Outcome.DETECTED : Outcome.CLEAR);
    }

    @Test
    void prevCloseDetectionUsesTheCurrentSnapshotTimeAsTheFirstCrossing() {
        Detection detection = prevClose(10_400, raw("10000")).detection();

        assertThat(detection.type()).isEqualTo(DetectionType.PREV_CLOSE);
        assertThat(detection.previousPrice()).isEqualTo(10_000L);
        assertThat(detection.changePercent()).isEqualByComparingTo("4.00");
        assertThat(detection.thresholdPercent()).isEqualByComparingTo("3.00");
        assertThat(detection.moveStartedAt()).isEqualTo(NOW);
    }

    @Test
    void prevCloseReadsNumbersWrittenAsStringsOrWithTrailingZeros() {
        assertThat(prevClose(10_400, raw("\"10000\"")).outcome()).isEqualTo(Outcome.DETECTED);
        assertThat(prevClose(10_400, raw("10000.0")).outcome()).isEqualTo(Outcome.DETECTED);
    }

    @ParameterizedTest(name = "raw {0} -> cannot be judged")
    @ValueSource(strings = {
            "{\"fake\":{}}",
            "{\"other\":1}",
            "{\"fake\":{\"prevClose\":null}}",
            "{\"fake\":{\"prevClose\":\"abc\"}}",
            "{\"fake\":{\"prevClose\":0}}",
            "{\"fake\":{\"prevClose\":-5}}",
            "{\"fake\":{\"prevClose\":10000.5}}",
            "<html>error</html>",
            "",
            "  "
    })
    void prevCloseWithMissingOrUnreadableFieldIsNotJudged(String raw) {
        assertThat(prevClose(10_400, raw).outcome()).isEqualTo(Outcome.UNKNOWN);
    }

    @Test
    void prevCloseWithoutRawIsNotJudged() {
        assertThat(prevClose(10_400, null).outcome()).isEqualTo(Outcome.UNKNOWN);
    }
}

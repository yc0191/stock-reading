package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import dev.stockreading.market.MarketProperties;
import dev.stockreading.market.snapshot.Evaluation.Outcome;
import tools.jackson.databind.json.JsonMapper;

class MoveDetectorTest {

    private static final OffsetDateTime NOW = OffsetDateTime.parse("2026-10-20T16:05:00+09:00");

    // 가짜 칸 이름이다. 실제 칸은 application.yml 의 market.first-trade-fields 로 정한다.
    private static final List<String> FIRST_TRADE_FIELDS = List.of("fake.volume", "fake.open");

    private PriceSnapshotRepository repository;
    private MoveDetector detector;

    @BeforeEach
    void setUp() {
        repository = mock(PriceSnapshotRepository.class);
        // 가짜 필드 이름이다. 실제 응답 필드는 사용자가 원문을 보고 정한다.
        MarketProperties properties = new MarketProperties(
                5, new BigDecimal("1.00"), List.of("fake.a", "fake.b", "fake.c"), new BigDecimal("3.00"), List.of(),
                3, FIRST_TRADE_FIELDS, 30, new BigDecimal("2.00"));
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

    // 세 후보 칸이 모두 같은 값인 응답.
    private static String raw(String prevClose) {
        return raw(prevClose, prevClose, prevClose);
    }

    private static String raw(String a, String b, String c) {
        return "{\"fake\":{\"a\":" + a + ",\"b\":" + b + ",\"c\":" + c + "},\"price\":\"1\"}";
    }

    @Test
    void windowQueryAlwaysHasBothBoundsOfTheConfiguredLength() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of());

        window(10_000);

        verify(repository).findBetween("TEST01", NOW.minusMinutes(5), NOW, FIRST_TRADE_FIELDS);
    }

    @Test
    void windowWithoutHistoryIsNotDetectedAndEndsAnyRunningMove() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of());

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
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 10_000)));

        assertThat(window(price).outcome()).isEqualTo(detected ? Outcome.DETECTED : Outcome.CLEAR);
    }

    @Test
    void windowDetectionCarriesBasePriceChangeAndThreshold() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 10_000)));

        Detection detection = window(9_850).detection();

        assertThat(detection.type()).isEqualTo(DetectionType.WINDOW);
        assertThat(detection.previousPrice()).isEqualTo(10_000L);
        assertThat(detection.price()).isEqualTo(9_850L);
        assertThat(detection.changePercent()).isEqualByComparingTo("-1.50");
        assertThat(detection.thresholdPercent()).isEqualByComparingTo("1.00");
    }

    @Test
    void windowMoveStartsAtTheLatestSnapshotBelowTheThreshold() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
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
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_000),
                point("16:01:00", 10_150),
                point("16:02:00", 10_200)));

        assertThat(window(10_250).detection().moveStartedAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T16:00:00+09:00"));
    }

    @Test
    void windowMoveStartIsTheOldestSnapshotWhenItIsTheOnlyOne() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 10_000)));

        assertThat(window(10_200).detection().moveStartedAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T16:00:00+09:00"));
    }

    @Test
    void windowWithNonPositiveBaselineCannotBeJudged() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 0)));

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
            "{\"fake\":{\"a\":null,\"b\":null,\"c\":null}}",
            "{\"fake\":{\"a\":\"abc\",\"b\":\"abc\",\"c\":\"abc\"}}",
            "{\"fake\":{\"a\":0,\"b\":0,\"c\":0}}",
            "{\"fake\":{\"a\":-5,\"b\":-5,\"c\":-5}}",
            "{\"fake\":{\"a\":10000.5,\"b\":10000.5,\"c\":10000.5}}",
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

    // 세 칸의 다수결: 같은 값이 두 칸 이상이면 그 값을 쓰고, 아니면 판단하지 않는다.
    static Stream<Object[]> votes() {
        return Stream.of(
                new Object[] {"세 칸이 같음", raw("10000", "10000", "10000"), 10_000L},
                new Object[] {"첫 칸만 다름", raw("12345", "10000", "10000"), 10_000L},
                new Object[] {"가운데 칸만 다름", raw("10000", "12345", "10000"), 10_000L},
                new Object[] {"끝 칸만 다름", raw("10000", "10000", "12345"), 10_000L},
                new Object[] {"한 칸이 비고 나머지 둘이 같음", raw("null", "10000", "10000"), 10_000L},
                new Object[] {"한 칸이 숫자가 아니고 나머지 둘이 같음", raw("10000", "\"abc\"", "10000"), 10_000L},
                new Object[] {"공백 붙은 문자열과 숫자가 같은 값", raw("\"  10000\"", "10000", "9999"), 10_000L},
                new Object[] {"표기가 다른 같은 값", raw("\"10000.0\"", "10000", "9999"), 10_000L},
                new Object[] {"세 칸이 모두 다름", raw("10000", "10100", "10200"), null},
                new Object[] {"한 칸이 비고 나머지 둘이 다름", raw("null", "10000", "10100"), null},
                new Object[] {"두 칸이 비고 한 칸만 있음", raw("null", "null", "10000"), null},
                new Object[] {"두 칸이 숫자가 아니고 한 칸만 있음", raw("\"x\"", "10000", "\"y\""), null});
    }

    @ParameterizedTest(name = "{0}")
    @org.junit.jupiter.params.provider.MethodSource("votes")
    void prevCloseTakesTheMajorityOfTheThreeFields(String name, String raw, Long expected) {
        Evaluation evaluation = prevClose(10_400, raw);

        if (expected == null) {
            assertThat(evaluation.outcome()).isEqualTo(Outcome.UNKNOWN);
        } else {
            assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
            assertThat(evaluation.detection().previousPrice()).isEqualTo(expected);
        }
    }

    @Test
    void prevCloseOfAWronglyReadFieldDoesNotRaiseAFalseMove() {
        // 한 칸이 깨져 현재가와 크게 다른 값이 와도 나머지 두 칸이 같으면 그 값으로 판단하므로 변동이 아니다.
        assertThat(prevClose(10_100, raw("7000", "10000", "10000")).outcome()).isEqualTo(Outcome.CLEAR);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.NullSource
    @org.junit.jupiter.params.provider.EmptySource
    void prevCloseIsOffWhenNoFieldIsSet(List<String> fields) {
        MarketProperties properties = new MarketProperties(
                5, new BigDecimal("1.00"), fields, new BigDecimal("3.00"), List.of(),
                3, FIRST_TRADE_FIELDS, 30, new BigDecimal("2.00"));
        MoveDetector off = new MoveDetector(repository, properties, JsonMapper.builder().build());
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 10_000)));

        List<Evaluation> evaluations = off.evaluate("TEST01", NOW, 10_400, raw("10000"));

        assertThat(evaluations.get(1).outcome()).isEqualTo(Outcome.UNKNOWN);
        assertThat(evaluations.get(0).outcome()).isEqualTo(Outcome.DETECTED);
        // 전일 종가를 읽지 않으므로 오늘 등락률도 없다.
        assertThat(evaluations.get(0).detection().todayChangePercent()).isNull();
    }

    // ---- 오늘 등락률: 감지한 스냅샷 원문의 전일 종가 대비 (#66)

    @Test
    void windowDetectionCarriesTodayChangeAgainstTheRawPrevClose() {
        // 최근 구간에서는 10000 → 10100 (+1.00%)로 올랐지만 전일 종가 10306 대비로는 -2.00%다.
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 10_000)));

        Detection detection = detector.evaluate("TEST01", NOW, 10_100, raw("10306")).get(0).detection();

        assertThat(detection.type()).isEqualTo(DetectionType.WINDOW);
        assertThat(detection.changePercent()).isEqualByComparingTo("1.00");
        assertThat(detection.todayChangePercent()).isEqualByComparingTo("-2.00");
    }

    @Test
    void todayChangeIsComputedFromThePrevClosePriceNotCopiedFromARateField() {
        // 원문의 등락률 칸(가짜 이름 fake.rate)은 옮기지 않는다. 다수결로 정한 전일 종가와 현재가로 코드가 계산한다.
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 10_000)));
        String rawWithRate = "{\"fake\":{\"a\":10000,\"b\":10000,\"c\":12345,\"rate\":\"-9.99\"},\"price\":\"1\"}";

        Detection detection = detector.evaluate("TEST01", NOW, 10_150, rawWithRate).get(0).detection();

        assertThat(detection.todayChangePercent()).isEqualByComparingTo("1.50");
    }

    @Test
    void windowDetectionHasNoTodayChangeWhenThePrevCloseIsUnknown() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(point("16:00:00", 10_000)));

        assertThat(window(10_200).detection().todayChangePercent()).isNull();
        assertThat(detector.evaluate("TEST01", NOW, 10_200, raw("10000", "10100", "10200")).get(0).detection()
                .todayChangePercent()).isNull();
    }

    @Test
    void prevCloseDetectionCarriesItsOwnChangeAsTodayChange() {
        Detection detection = prevClose(10_400, raw("10000")).detection();

        assertThat(detection.todayChangePercent()).isEqualByComparingTo("4.00");
    }

    // ---- 기준값에서 뺄 이력: 첫 체결 전 응답, 연속 확인을 받지 못한 튄 점

    private static PricePoint beforeTrade(String time, long price) {
        return new PricePoint(OffsetDateTime.parse("2026-10-20T" + time + "+09:00"), price, true);
    }

    private void priorRows(PricePoint... points) {
        when(repository.findLatestBetween(anyString(), any(), any(), anyInt(), anyList())).thenReturn(List.of(points));
    }

    @Test
    void rowsBeforeTheFirstTradeAreNotUsedAsTheBaseline() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                beforeTrade("16:00:00", 10_000), beforeTrade("16:00:01", 10_000),
                point("16:00:02", 10_153), point("16:00:03", 10_153), point("16:00:04", 10_153)));

        // 첫 체결 전 값(10000) 기준이면 +1.53%지만 첫 체결 값(10153) 기준이라 변동이 아니다.
        Evaluation evaluation = window(10_156);

        assertThat(evaluation.outcome()).isEqualTo(Outcome.CLEAR);
    }

    @Test
    void historyOfOnlyRowsBeforeTheFirstTradeHasNoBaseline() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                beforeTrade("16:00:00", 10_000), beforeTrade("16:00:01", 10_000)));

        assertThat(window(10_500).outcome()).isEqualTo(Outcome.CLEAR);
    }

    @Test
    void aSpikedOldestRowBetweenDifferentNeighboursIsNotTheBaseline() {
        // 창 앞의 값(10000)과 뒤의 값(10000)이 모두 튄 행(10755)과 다르다.
        priorRows(point("15:59:58", 10_000), point("15:59:59", 10_000));
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_755), point("16:00:01", 10_000), point("16:00:02", 10_000), point("16:00:03", 10_000)));

        Evaluation evaluation = window(10_000);

        assertThat(evaluation.outcome()).isEqualTo(Outcome.CLEAR);
    }

    @Test
    void aSpikeOfTwoRowsIsStillNotTheBaselineButThreeRowsAre() {
        priorRows(point("15:59:58", 10_000), point("15:59:59", 10_000));
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_755), point("16:00:01", 10_755), point("16:00:02", 10_000), point("16:00:03", 10_000)));
        assertThat(window(10_000).outcome()).isEqualTo(Outcome.CLEAR);

        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_755), point("16:00:01", 10_755), point("16:00:02", 10_755), point("16:00:03", 10_000)));
        Evaluation evaluation = window(10_000);

        assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
        assertThat(evaluation.detection().previousPrice()).isEqualTo(10_755L);
    }

    @Test
    void anOldestRowThatContinuesTheValuesBeforeTheWindowIsKept() {
        // 창 앞의 두 행이 같은 값(10000)이라 이 값은 세 건 이어졌다. 가장 오래된 행이 기준이다.
        priorRows(point("15:59:58", 10_000), point("15:59:59", 10_000));
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_000), point("16:00:01", 10_200), point("16:00:02", 10_200), point("16:00:03", 10_200)));

        Evaluation evaluation = window(10_200);

        assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
        assertThat(evaluation.detection().previousPrice()).isEqualTo(10_000L);
    }

    @Test
    void anOldestRowWithNothingBeforeTheWindowIsKeptBecauseItCannotBeJudged() {
        priorRows();
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_755), point("16:00:01", 10_000), point("16:00:02", 10_000)));

        Evaluation evaluation = window(10_000);

        assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
        assertThat(evaluation.detection().previousPrice()).isEqualTo(10_755L);
    }

    @Test
    void theValuesBeforeTheWindowAreNotReadWhenTheOldestRowIsAlreadyConfirmed() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_000), point("16:00:01", 10_000), point("16:00:02", 10_000), point("16:00:03", 10_300)));

        window(10_300);

        verify(repository, never()).findLatestBetween(anyString(), any(), any(), anyInt(), anyList());
    }

    @Test
    void theValuesBeforeTheWindowAreReadFromTheStartOfTheWindowBackByOneWindowLength() {
        when(repository.findBetween(anyString(), any(), any(), anyList())).thenReturn(List.of(
                point("16:00:00", 10_755), point("16:00:01", 10_000)));
        priorRows();

        window(10_000);

        verify(repository).findLatestBetween("TEST01", NOW.minusMinutes(10), NOW.minusMinutes(5), 2, FIRST_TRADE_FIELDS);
    }
}

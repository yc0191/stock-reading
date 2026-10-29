package dev.stockreading.market.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/** 시각은 모두 UTC로 쓴다. 2026-10월의 뉴욕 정규장 마감 16:00은 UTC 20:00이다. */
class OverseasCloseRateQueryTest {

    private static final String TICKER = "TESTA";
    // 행을 받은 시각(테스트 값). 실제 수집은 평일 장 시작 전 한 번이다.
    private static final OffsetDateTime FETCHED_1021 = OffsetDateTime.parse("2026-10-21T07:30:00Z");
    private static final OffsetDateTime FETCHED_1022 = OffsetDateTime.parse("2026-10-22T07:30:00Z");
    private static final OffsetDateTime FETCHED_1023 = OffsetDateTime.parse("2026-10-23T07:30:00Z");

    private InsightRepository repository;
    private OverseasCloseRateQuery query;

    @BeforeEach
    void setUp() {
        repository = mock(InsightRepository.class);
        query = new OverseasCloseRateQuery(repository, JsonMapper.builder().build());
    }

    @Test
    void picksTheLatestClosedSessionNotTheFirstRecordOrTheTopLevelRate() {
        // 맨 앞은 아직 진행 중인 10/21 장외 막대이고, 최상위 등락율도 그 막대 기준이다.
        rows(new OverseasDailyRow(response(" 9.99", "20261021:  0.10", "20261020:  1.25", "20261019: -0.40"), FETCHED_1021));

        OverseasCloseRate rate = query.find(TICKER, OffsetDateTime.parse("2026-10-21T08:00:00Z")).orElseThrow();

        assertThat(rate.date()).isEqualTo("20261020");
        assertThat(rate.rate()).isEqualTo("  1.25");
        assertThat(rate.closeAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T16:00:00-04:00"));
        assertThat(rate.ticker()).isEqualTo(TICKER);
        assertThat(rate.fetchedAt()).isEqualTo(FETCHED_1021);
    }

    @Test
    void sessionClosingExactlyAtTheReferenceTimeIsIncludedButNotOneSecondLater() {
        rows(new OverseasDailyRow(response(" 0.00", "20261020:  1.25", "20261019: -0.40"), FETCHED_1021));

        assertThat(query.find(TICKER, OffsetDateTime.parse("2026-10-20T20:00:00Z")).orElseThrow().date())
                .isEqualTo("20261020");
        assertThat(query.find(TICKER, OffsetDateTime.parse("2026-10-20T19:59:59Z")).orElseThrow().date())
                .isEqualTo("20261019");
    }

    @Test
    void barThatWasStillOpenWhenFetchedIsNotUsedAndTheOlderSessionIsReturnedWithItsDate() {
        // 10/21 장이 끝난 뒤가 기준이지만, 10/21 마감 뒤에 받은 행이 아직 없다.
        rows(new OverseasDailyRow(response(" 0.10", "20261021:  0.10", "20261020:  1.25"), FETCHED_1021));

        OverseasCloseRate rate = query.find(TICKER, OffsetDateTime.parse("2026-10-21T21:00:00Z")).orElseThrow();

        assertThat(rate.date()).isEqualTo("20261020");
        assertThat(rate.rate()).isEqualTo("  1.25");
    }

    @Test
    void sameDateInSeveralRowsUsesTheLatestFetchedValueEvenIfFetchedAfterTheReferenceTime() {
        rows(
                new OverseasDailyRow(response(" 0.30", "20261023:  0.30", "20261022:  2.10", "20261021: -0.50"), FETCHED_1023),
                new OverseasDailyRow(response(" 0.20", "20261022:  0.20", "20261021: -0.55", "20261020:  1.25"), FETCHED_1022));

        OverseasCloseRate rate = query.find(TICKER, OffsetDateTime.parse("2026-10-21T21:00:00Z")).orElseThrow();

        assertThat(rate.date()).isEqualTo("20261021");
        assertThat(rate.rate()).isEqualTo(" -0.50");
        assertThat(rate.fetchedAt()).isEqualTo(FETCHED_1023);
    }

    @Test
    void rowOrderFromTheStoreDoesNotDecideWhichSameDateValueWins() {
        rows(
                new OverseasDailyRow(response(" 0.20", "20261021: -0.55"), FETCHED_1022),
                new OverseasDailyRow(response(" 0.30", "20261021: -0.50"), FETCHED_1023));

        assertThat(query.find(TICKER, OffsetDateTime.parse("2026-10-22T00:00:00Z")).orElseThrow().rate())
                .isEqualTo(" -0.50");
    }

    @Test
    void recordsWithoutADateOrRateAndRowsThatAreNotJsonAreSkipped() {
        rows(
                new OverseasDailyRow("not json", FETCHED_1022),
                new OverseasDailyRow("""
                        {"dataBody":{"out2":[{"dt":"20261020"},{"dt":"bad","up_dwn_r_p2":" 1.00"},
                         {"up_dwn_r_p2":" 2.00"},{"dt":"20261019","up_dwn_r_p2":" -0.40"}]}}""", FETCHED_1021));

        assertThat(query.find(TICKER, OffsetDateTime.parse("2026-10-21T08:00:00Z")).orElseThrow().rate())
                .isEqualTo(" -0.40");
    }

    @Test
    void returnsNothingWhenNoSessionClosedBeforeTheReferenceTime() {
        rows(new OverseasDailyRow(response(" 1.25", "20261020:  1.25"), FETCHED_1021));

        assertThat(query.find(TICKER, OffsetDateTime.parse("2026-10-20T12:00:00Z"))).isEmpty();
    }

    @Test
    void readsOnlyTheRecentRowsOfTheTicker() {
        rows();

        assertThat(query.find(TICKER, FETCHED_1021)).isEmpty();
        verify(repository).recentOverseasDailyPrices(TICKER, OverseasCloseRateQuery.ROWS_TO_READ);
        assertThat(OverseasCloseRateQuery.ROWS_TO_READ).isEqualTo(10);
    }

    @Test
    void classShareWrittenWithADotIsLookedUpWithTheStoredDash() {
        when(repository.recentOverseasDailyPrices(eq("TESTB-X"), anyInt())).thenReturn(List.of(
                new OverseasDailyRow(response(" 9.99", "20261020:  1.25"), FETCHED_1021)));

        OverseasCloseRate rate = query.find("testb.x", OffsetDateTime.parse("2026-10-21T08:00:00Z")).orElseThrow();

        assertThat(rate.ticker()).isEqualTo("TESTB-X");
        verify(repository).recentOverseasDailyPrices("TESTB-X", OverseasCloseRateQuery.ROWS_TO_READ);
    }

    private void rows(OverseasDailyRow... rows) {
        when(repository.recentOverseasDailyPrices(eq(TICKER), anyInt())).thenReturn(List.of(rows));
    }

    /** "일자:등락율" 목록으로 응답 원문을 만든다. 최상위에도 등락율이 있어 이를 쓰지 않는지 확인할 수 있다. */
    private static String response(String topLevelRate, String... records) {
        String out2 = Arrays.stream(records)
                .map(r -> r.split(":", 2))
                .map(r -> "{\"dt\":\"" + r[0] + "\",\"tm\":\"000000\",\"up_dwn_r_p2\":\"" + r[1] + "\"}")
                .collect(Collectors.joining(","));
        return "{\"dataHeader\":{\"resultCode\":\"200\"},\"dataBody\":{\"up_dwn_r_p2\":\"" + topLevelRate
                + "\",\"out2\":[" + out2 + "]}}";
    }
}

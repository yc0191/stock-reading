package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.collector.holdings.HoldingsPort;

class FilingCollectorTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-21T00:00:00Z"), ZoneId.of("Asia/Seoul"));

    private final EdgarPort edgar = mock(EdgarPort.class);
    private final HoldingsPort holdings = mock(HoldingsPort.class);

    private FilingCollector collector(String userAgent) {
        var properties = new EdgarProperties("http://unused", "http://unused", "https://www.sec.gov/Archives/edgar/data",
                userAgent, 5, 10, "8-K,8-K/A,6-K", 72);
        return new FilingCollector(edgar, holdings, properties, CLOCK);
    }

    @Test
    void companyWithTwoShareClassesIsQueriedOnceAndCarriesBothTickers() {
        when(holdings.allTickers()).thenReturn(List.of("TESTB", "TESTA", "TESTB.C"));
        when(edgar.cikOf("TESTA")).thenReturn("0000000001");
        when(edgar.cikOf("TESTB")).thenReturn("0000000002");
        when(edgar.cikOf("TESTB.C")).thenReturn("0000000002");
        when(edgar.submissions("0000000001")).thenReturn(submissions("0000000001",
                filing("0000000001-26-000001", "8-K", "2026-10-20T20:30:28Z")));
        when(edgar.submissions("0000000002")).thenReturn(submissions("0000000002",
                filing("0000000002-26-000001", "6-K", "2026-10-20T10:00:00Z"),
                filing("0000000002-26-000002", "10-K", "2026-10-20T11:00:00Z")));

        List<CollectedFiling> result = collector("TestCompany test-contact@example.com").collect();

        verify(edgar, times(1)).submissions("0000000002");
        // 접수 시각 오름차순
        assertThat(result).extracting(c -> c.filing().accessionNumber())
                .containsExactly("0000000002-26-000001", "0000000001-26-000001");
        assertThat(result.get(0).tickers()).containsExactly("TESTB", "TESTB.C");
        assertThat(result.get(0).cik()).isEqualTo("0000000002");
        assertThat(result.get(1).tickers()).containsExactly("TESTA");
    }

    @Test
    void unknownTickerOrFailedCompanyDoesNotStopTheOthers() {
        when(holdings.allTickers()).thenReturn(List.of("TESTX", "TESTA", "TESTB"));
        when(edgar.cikOf("TESTX")).thenThrow(new EdgarApiException(EdgarFailure.UNKNOWN_TICKER, null));
        when(edgar.cikOf("TESTA")).thenReturn("0000000001");
        when(edgar.cikOf("TESTB")).thenReturn("0000000002");
        when(edgar.submissions("0000000001")).thenThrow(new EdgarApiException(EdgarFailure.SERVER_ERROR, 503));
        when(edgar.submissions("0000000002")).thenReturn(submissions("0000000002",
                filing("0000000002-26-000001", "8-K", "2026-10-20T10:00:00Z")));

        List<CollectedFiling> result = collector("TestCompany test-contact@example.com").collect();

        assertThat(result).extracting(c -> c.filing().accessionNumber()).containsExactly("0000000002-26-000001");
    }

    @Test
    void tickerMapFailureStopsTheWholeRunBecauseEveryTickerWouldFailTheSameWay() {
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB"));
        when(edgar.cikOf(anyString())).thenThrow(new EdgarApiException(EdgarFailure.NETWORK, null));

        assertThatThrownBy(() -> collector("TestCompany test-contact@example.com").collect())
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.NETWORK));
        verify(edgar, times(1)).cikOf(anyString());
    }

    @Test
    void missingUserAgentFailsBeforeAnyCall() {
        when(holdings.allTickers()).thenReturn(List.of("TESTA"));

        assertThatThrownBy(() -> collector("").collect())
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.NOT_CONFIGURED));
        verify(edgar, never()).cikOf(anyString());
    }

    @Test
    void noHoldingsMeansNoCallsEvenWithoutUserAgent() {
        when(holdings.allTickers()).thenReturn(List.of());

        assertThat(collector("").collect()).isEmpty();
        verify(edgar, never()).cikOf(anyString());
    }

    private static CompanySubmissions submissions(String cik, Filing... filings) {
        return new CompanySubmissions(cik, List.of("Nasdaq"), List.of(filings));
    }

    private static Filing filing(String accession, String form, String acceptedAt) {
        return new Filing(accession, form, OffsetDateTime.parse(acceptedAt), LocalDate.of(2026, 10, 20),
                List.of("8.01"), "https://www.sec.gov/Archives/edgar/data/1/x/test.htm");
    }
}

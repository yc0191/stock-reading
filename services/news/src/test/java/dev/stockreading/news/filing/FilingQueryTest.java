package dev.stockreading.news.filing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FilingQueryTest {

    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-21T14:00:00Z");

    private FilingRepository repository;
    private FilingQuery query;

    @BeforeEach
    void setUp() {
        repository = mock(FilingRepository.class);
        query = new FilingQuery(repository);
    }

    @Test
    void periodEndingAfterTheReferenceTimeIsCutAtTheReferenceTime() {
        OffsetDateTime from = AS_OF.minusHours(24);

        query.find("TESTA", from, AS_OF.plusHours(6), AS_OF);

        verify(repository).findByTicker("TESTA", from, AS_OF);
    }

    @Test
    void classShareWrittenWithADotIsLookedUpWithTheStoredDash() {
        OffsetDateTime from = AS_OF.minusHours(24);

        query.find(" testb.x ", from, AS_OF, AS_OF);

        verify(repository).findByTicker("TESTB-X", from, AS_OF);
    }

    @Test
    void periodEndingBeforeTheReferenceTimeKeepsItsEnd() {
        OffsetDateTime from = AS_OF.minusHours(48);
        OffsetDateTime to = AS_OF.minusHours(24);

        query.find("TESTA", from, to, AS_OF);

        verify(repository).findByTicker("TESTA", from, to);
    }

    @Test
    void filingsJustBeforeAndExactlyAtTheReferenceTimeAreReturnedButNotOneJustAfter() {
        OffsetDateTime from = AS_OF.minusHours(24);
        StoredFiling before = filing("0000000001-26-000001", AS_OF.minusSeconds(1));
        StoredFiling exact = filing("0000000001-26-000002", AS_OF);
        StoredFiling after = filing("0000000001-26-000003", AS_OF.plusSeconds(1));
        // 저장소가 상한을 어기고 돌려줘도 기준 시각 뒤의 공시는 나가지 않아야 한다.
        when(repository.findByTicker(eq("TESTA"), any(), any())).thenReturn(List.of(before, exact, after));

        List<StoredFiling> found = query.find("TESTA", from, AS_OF.plusHours(1), AS_OF);

        assertThat(found).containsExactly(before, exact);
    }

    @Test
    void sameInstantInAnotherOffsetIsTreatedAsEqual() {
        StoredFiling exact = filing("0000000001-26-000002", OffsetDateTime.parse("2026-10-21T23:00:00+09:00"));
        when(repository.findByTicker(eq("TESTA"), any(), any())).thenReturn(List.of(exact));

        assertThat(query.find("TESTA", AS_OF.minusHours(1), AS_OF, AS_OF)).containsExactly(exact);
    }

    @Test
    void periodStartingAfterTheReferenceTimeReturnsNothingWithoutReading() {
        assertThat(query.find("TESTA", AS_OF.plusSeconds(1), AS_OF.plusHours(1), AS_OF)).isEmpty();

        verify(repository, never()).findByTicker(anyString(), any(), any());
    }

    @Test
    void doesNotImposeItsOwnAgeLimit() {
        // 24시간 한도는 호출자가 from으로 정한다. 조회 계층이 기간을 줄이지 않아야 한다.
        OffsetDateTime from = AS_OF.minusDays(3);

        query.find("TESTA", from, AS_OF, AS_OF);

        verify(repository).findByTicker("TESTA", from, AS_OF);
    }

    @Test
    void rejectsAnInvertedPeriod() {
        assertThatThrownBy(() -> query.find("TESTA", AS_OF, AS_OF.minusSeconds(1), AS_OF))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static StoredFiling filing(String accession, OffsetDateTime acceptedAt) {
        return new StoredFiling(accession, "0000000001", "8-K", List.of("2.02"), acceptedAt,
                LocalDate.parse("2026-10-21"), "https://www.sec.gov/Archives/edgar/data/1/x/test.htm", List.of("TESTA"));
    }
}

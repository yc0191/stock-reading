package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class FilingFilterTest {

    private static final Instant NOW = Instant.parse("2026-10-21T00:00:00Z");

    @Test
    void keepsOnlyConfiguredFormsAcceptedWithinTheLookback() {
        List<Filing> filings = List.of(
                filing("0000000001-26-000001", "8-K", "2026-10-20T20:30:28Z"),
                filing("0000000001-26-000002", "10-Q", "2026-10-20T20:30:28Z"),
                filing("0000000001-26-000003", "6-K", "2026-10-17T23:59:59Z"),
                filing("0000000001-26-000004", "8-K/A", "2026-10-18T00:00:00Z"));

        FilingFilter.Result result = FilingFilter.apply(filings, Set.of("8-K", "8-K/A", "6-K"), NOW, Duration.ofHours(72));

        assertThat(result.passed()).extracting(Filing::accessionNumber)
                .containsExactly("0000000001-26-000001", "0000000001-26-000004");
        assertThat(result.otherForm()).isEqualTo(1);
        assertThat(result.tooOld()).isEqualTo(1);
    }

    @Test
    void formMatchIsExactSoAmendmentsNeedTheirOwnEntry() {
        FilingFilter.Result result = FilingFilter.apply(
                List.of(filing("0000000001-26-000004", "8-K/A", "2026-10-20T00:00:00Z")), Set.of("8-K"), NOW,
                Duration.ofHours(72));

        assertThat(result.passed()).isEmpty();
        assertThat(result.otherForm()).isEqualTo(1);
    }

    private static Filing filing(String accession, String form, String acceptedAt) {
        return new Filing(accession, form, OffsetDateTime.parse(acceptedAt), LocalDate.of(2026, 10, 20), List.of(),
                "https://www.sec.gov/Archives/edgar/data/1/x/test.htm");
    }
}

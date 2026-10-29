package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class OverseasExchangeTest {

    @Test
    void secExchangeNamesMapToQuoteExchangeCodes() {
        assertThat(OverseasExchange.codeOf(List.of("Nasdaq"))).isEqualTo("NAS");
        assertThat(OverseasExchange.codeOf(List.of("NYSE"))).isEqualTo("NYS");
        assertThat(OverseasExchange.codeOf(List.of("NYSE American"))).isEqualTo("AMX");
        assertThat(OverseasExchange.codeOf(List.of("NYSE MKT"))).isEqualTo("AMX");
        assertThat(OverseasExchange.codeOf(List.of(" nasdaq "))).isEqualTo("NAS");
    }

    @Test
    void firstMappableExchangeWins() {
        assertThat(OverseasExchange.codeOf(List.of("OTC", "NYSE", "Nasdaq"))).isEqualTo("NYS");
    }

    @Test
    void unknownOrMissingExchangeGivesNull() {
        assertThat(OverseasExchange.codeOf(List.of("OTC", "CBOE"))).isNull();
        assertThat(OverseasExchange.codeOf(List.of())).isNull();
        assertThat(OverseasExchange.codeOf(Arrays.asList((String) null))).isNull();
        assertThat(OverseasExchange.codeOf(null)).isNull();
    }
}

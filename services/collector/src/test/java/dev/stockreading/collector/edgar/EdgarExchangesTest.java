package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class EdgarExchangesTest {

    private final EdgarPort edgar = mock(EdgarPort.class);
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-21T00:00:00Z"));
    private final Clock clock = new Clock() {
        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    };

    @Test
    void returnsTheSubmissionsExchangesAndReusesThemForADay() {
        when(edgar.cikOf("TESTA")).thenReturn("0000000001");
        when(edgar.submissions("0000000001"))
                .thenReturn(new CompanySubmissions("0000000001", List.of("Nasdaq"), List.of()));
        ExchangePort port = new EdgarExchanges(edgar, clock);

        assertThat(port.exchanges("TESTA")).containsExactly("Nasdaq");
        now.set(now.get().plus(Duration.ofHours(23)));
        assertThat(port.exchanges("TESTA")).containsExactly("Nasdaq");
        verify(edgar, times(1)).submissions("0000000001");

        now.set(now.get().plus(Duration.ofHours(2)));
        port.exchanges("TESTA");
        verify(edgar, times(2)).submissions("0000000001");
    }

    @Test
    void failureIsPassedToTheCallerWithItsKind() {
        when(edgar.cikOf("TESTZ")).thenThrow(new EdgarApiException(EdgarFailure.UNKNOWN_TICKER, null));

        assertThatThrownBy(() -> new EdgarExchanges(edgar, clock).exchanges("TESTZ"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.UNKNOWN_TICKER));
    }
}

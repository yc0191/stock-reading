package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;

class EdgarExchangesTest {

    private final EdgarPort edgar = mock(EdgarPort.class);

    @Test
    void returnsTheExchangeFromTheTickerMapWithoutReadingSubmissions() {
        when(edgar.exchangesOf("TESTA")).thenReturn(List.of("Nasdaq"));

        assertThat(new EdgarExchanges(edgar).exchanges("TESTA")).containsExactly("Nasdaq");
        verify(edgar, never()).submissions(anyString());
    }

    @Test
    void failureIsPassedToTheCallerWithItsKind() {
        when(edgar.exchangesOf("TESTZ")).thenThrow(new EdgarApiException(EdgarFailure.UNKNOWN_TICKER, null));

        assertThatThrownBy(() -> new EdgarExchanges(edgar).exchanges("TESTZ"))
                .isInstanceOfSatisfying(EdgarApiException.class,
                        e -> assertThat(e.failure()).isEqualTo(EdgarFailure.UNKNOWN_TICKER));
    }
}

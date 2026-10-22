package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;

class BrokerMarketDataTest {

    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-22T10:15:30+09:00");

    @Test
    void sourceNameIsBroker() {
        assertThat(new BrokerMarketData(mock(BrokerClient.class)).sourceName()).isEqualTo("broker");
    }

    @Test
    void skipsFailedSymbolAndKeepsTheRest() {
        BrokerClient client = mock(BrokerClient.class);
        when(client.currentPrice(anyString())).thenAnswer(invocation -> {
            String symbol = invocation.getArgument(0);
            if (symbol.equals("BAD001")) {
                throw new BrokerApiException("boom");
            }
            return new PriceQuote(symbol, 1_000L, AS_OF);
        });

        List<PriceQuote> quotes = new BrokerMarketData(client).fetch(List.of("TEST01", "BAD001", "TEST02"));

        assertThat(quotes).extracting(PriceQuote::symbol).containsExactly("TEST01", "TEST02");
    }
}

package dev.stockreading.collector.marketdata;

import java.time.OffsetDateTime;

public record PriceQuote(String symbol, long price, OffsetDateTime asOf) {
}

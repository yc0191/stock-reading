package dev.stockreading.collector.marketdata;

import java.time.OffsetDateTime;

/** raw는 시세 응답 원문(trim만 적용)이다. 시뮬레이터처럼 원문이 없으면 null이다. */
public record PriceQuote(String symbol, long price, OffsetDateTime asOf, String raw) {

    public PriceQuote(String symbol, long price, OffsetDateTime asOf) {
        this(symbol, price, asOf, null);
    }
}

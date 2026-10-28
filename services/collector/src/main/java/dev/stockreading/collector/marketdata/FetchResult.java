package dev.stockreading.collector.marketdata;

import java.util.List;

/** 한 tick의 조회 결과. 성공하지 못한 종목은 조용히 빠지지 않고 failures 에 들어간다. */
public record FetchResult(List<PriceQuote> quotes, List<QuoteFailure> failures) {

    public static FetchResult ofQuotes(List<PriceQuote> quotes) {
        return new FetchResult(quotes, List.of());
    }
}

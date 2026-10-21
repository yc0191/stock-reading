package dev.stockreading.collector.marketdata;

import java.util.List;

/** 시세 출처. 실제 증권사 어댑터가 붙기 전까지는 시뮬레이터를 쓴다. */
public interface MarketDataPort {

    String sourceName();

    List<PriceQuote> fetch(List<String> symbols);
}

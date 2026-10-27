package dev.stockreading.collector.marketdata;

import java.util.List;

/** 시세 출처. 실제 시세 제공자 어댑터가 붙기 전까지는 시뮬레이터를 쓴다. */
public interface MarketDataPort {

    String sourceName();

    FetchResult fetch(List<String> symbols);
}

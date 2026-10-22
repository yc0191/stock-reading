package dev.stockreading.collector.marketdata;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
class BrokerMarketData implements MarketDataPort {

    private static final Logger log = LoggerFactory.getLogger(BrokerMarketData.class);

    private final BrokerClient client;

    BrokerMarketData(BrokerClient client) {
        this.client = client;
    }

    @Override
    public String sourceName() {
        return "broker";
    }

    // 한 종목이 실패해도 나머지 종목은 발행되도록 실패한 종목만 건너뛴다.
    @Override
    public List<PriceQuote> fetch(List<String> symbols) {
        List<PriceQuote> quotes = new ArrayList<>();
        for (String symbol : symbols) {
            try {
                quotes.add(client.currentPrice(symbol));
            } catch (BrokerApiException e) {
                log.warn("quote failed symbol={} reason={}", symbol, e.getMessage());
            }
        }
        return quotes;
    }
}

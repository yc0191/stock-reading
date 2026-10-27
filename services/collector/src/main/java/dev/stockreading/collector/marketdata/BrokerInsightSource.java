package dev.stockreading.collector.marketdata;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.insight.InsightProperties;
import dev.stockreading.collector.insight.InsightSourcePort;

// BrokerClient 가 패키지 공개라서 insight 패키지가 직접 쓸 수 없다. 이 클래스가 조회 메서드만 열어 준다.
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
class BrokerInsightSource implements InsightSourcePort {

    private final BrokerClient client;
    private final InsightProperties properties;

    BrokerInsightSource(BrokerClient client, InsightProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    @Override
    public String dailyPrice(String symbol) {
        return client.dailyPriceRaw(symbol, properties.dailyMarketClass(), properties.dailyCount());
    }

    @Override
    public String worldIndex(String continent) {
        return client.worldIndexRaw(continent);
    }

    @Override
    public String exchangeRateSummary() {
        return client.exchangeRateSummaryRaw();
    }

    @Override
    public String marketSummary() {
        return client.marketSummaryRaw();
    }
}

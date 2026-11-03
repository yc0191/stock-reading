package dev.stockreading.collector.marketdata;

import java.time.Duration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.insight.InsightProperties;
import dev.stockreading.collector.insight.InsightSourcePort;

// BrokerClient 가 패키지 공개라서 insight 패키지가 직접 쓸 수 없다. 이 클래스가 조회 메서드만 열어 준다.
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
class BrokerInsightSource implements InsightSourcePort {

    // 장 마감 후 조회는 종목 수만큼 연달아 호출하므로 1초 시세 조회보다 길게 자리를 기다린다.
    static final Duration LIMITER_WAIT = Duration.ofSeconds(2);

    private final BrokerClient client;
    private final InsightProperties properties;
    private final CallRateLimit limiter;

    BrokerInsightSource(BrokerClient client, InsightProperties properties, CallRateLimit limiter) {
        this.client = client;
        this.properties = properties;
        this.limiter = limiter;
    }

    // 모든 조회가 시세 조회와 같은 상한 장치를 거친다. 따로 세면 둘이 겹칠 때 앱 전체 한도를 넘을 수 있다.
    @Override
    public String dailyPrice(String symbol) {
        acquire();
        return client.dailyPriceRaw(symbol, properties.dailyMarketClass(), properties.dailyCount());
    }

    @Override
    public String overseasDailyPrice(String exchangeCode, String ticker) {
        acquire();
        return client.overseasDailyPriceRaw(exchangeCode, ticker, properties.overseasDailyCount());
    }

    @Override
    public String worldIndex(String continent) {
        acquire();
        return client.worldIndexRaw(continent);
    }

    @Override
    public String exchangeRateSummary() {
        acquire();
        return client.exchangeRateSummaryRaw();
    }

    @Override
    public String marketSummary() {
        acquire();
        return client.marketSummaryRaw();
    }

    // 자리를 얻지 못하면 서버를 부르지 않고 그 조회만 실패시킨다. 발행 쪽이 건별로 잡으므로 나머지 조회는 계속된다.
    private void acquire() {
        if (!limiter.tryAcquire(LIMITER_WAIT)) {
            throw new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과",
                    null, null, BrokerMarketData.RATE_LIMITED, null);
        }
    }
}

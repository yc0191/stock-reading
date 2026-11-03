package dev.stockreading.collector.marketdata;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class CallRateLimitConfig {

    // Redis 구현을 추가할 때는 저장소를 고르는 속성을 달아 둘 중 하나만 만들어지게 한다.
    @Bean
    @ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
    CallRateLimit callRateLimit(BrokerProperties broker) {
        return CallRateLimiter.perSecond(broker.maxCallsPerSecond());
    }
}

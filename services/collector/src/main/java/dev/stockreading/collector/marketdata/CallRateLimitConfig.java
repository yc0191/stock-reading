package dev.stockreading.collector.marketdata;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class CallRateLimitConfig {

    // Redis 구현을 추가할 때는 저장소를 고르는 속성을 달아 둘 중 하나만 만들어지게 한다.
    // BrokerClient 가 모든 모드에서 만들어지고 재시도 때 이 상한을 쓰므로 모드와 상관없이 만든다.
    @Bean
    CallRateLimit callRateLimit(BrokerProperties broker) {
        return CallRateLimiter.perSecond(broker.maxCallsPerSecond());
    }
}

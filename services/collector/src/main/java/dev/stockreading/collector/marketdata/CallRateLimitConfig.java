package dev.stockreading.collector.marketdata;

import dev.stockreading.collector.CollectorProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class CallRateLimitConfig {

    // 회차당 호출 수가 상한을 넘으면 시작할 때 실패하게 한다.
    @Bean
    QuotePerTick quotePerTick(CollectorProperties collector, BrokerProperties broker) {
        return QuotePerTick.checked(collector.quotePerTick(), broker.maxCallsPerSecond());
    }

    // Redis 구현을 추가할 때는 저장소를 고르는 속성을 달아 둘 중 하나만 만들어지게 한다.
    // BrokerClient 가 모든 모드에서 만들어지고 재시도 때 이 상한을 쓰므로 모드와 상관없이 만든다.
    @Bean
    CallRateLimit callRateLimit(BrokerProperties broker) {
        return CallRateLimiter.perSecond(broker.maxCallsPerSecond());
    }
}

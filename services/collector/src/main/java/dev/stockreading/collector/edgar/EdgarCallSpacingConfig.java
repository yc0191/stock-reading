package dev.stockreading.collector.edgar;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class EdgarCallSpacingConfig {

    // 모든 EDGAR 호출이 같은 간격 장치를 거쳐야 초당 상한이 지켜지므로 하나만 만든다.
    @Bean
    EdgarCallSpacing edgarCallSpacing(EdgarProperties properties) {
        return EdgarCallSpacing.perSecond(properties.maxCallsPerSecond());
    }
}

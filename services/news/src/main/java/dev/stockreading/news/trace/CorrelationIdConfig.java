package dev.stockreading.news.trace;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.RecordInterceptor;

/** 기본 리스너 컨테이너 팩토리가 이 빈을 가져가 모든 리스너에 건다. 따로 만든 팩토리는 직접 걸어야 한다. */
@Configuration
class CorrelationIdConfig {

    @Bean
    RecordInterceptor<Object, Object> correlationIdRecordInterceptor() {
        return new CorrelationIdRecordInterceptor<>();
    }
}

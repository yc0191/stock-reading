package dev.stockreading.news.article;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class ErrorHandlerConfig {

    /** 간격 없이 재시도하는 횟수. 기본 오류 처리기와 같은 값을 유지한다. */
    static final long RETRIES = 9;

    /** 최초 전달 1회와 재시도를 합친 총 시도 횟수. */
    static final int DELIVERY_ATTEMPTS = (int) RETRIES + 1;

    @Bean
    DefaultErrorHandler kafkaErrorHandler(FailedEventRecoverer recoverer) {
        return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, RETRIES));
    }
}

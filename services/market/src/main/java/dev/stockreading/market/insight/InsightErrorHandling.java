package dev.stockreading.market.insight;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class InsightErrorHandling {

    /** 간격 없이 재시도하는 횟수. 기본 오류 처리기와 같은 값을 유지한다. */
    static final long RETRIES = 9;

    /** 최초 전달 1회와 재시도를 합친 총 시도 횟수. */
    static final int DELIVERY_ATTEMPTS = (int) RETRIES + 1;

    /** DefaultErrorHandler를 빈으로 두지 않는다. 빈이면 기본 컨테이너 팩토리(market.snapshot)에도 적용되어 기존 처리가 바뀐다. */
    static DefaultErrorHandler errorHandler(FailedEventRecoverer recoverer) {
        return new DefaultErrorHandler(recoverer, new FixedBackOff(0L, RETRIES));
    }

    @Bean(InsightListener.CONTAINER_FACTORY)
    ConcurrentKafkaListenerContainerFactory<String, String> insightListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, FailedEventRecoverer recoverer) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler(recoverer));
        return factory;
    }
}

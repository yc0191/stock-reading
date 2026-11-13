package dev.stockreading.market.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;

import dev.stockreading.market.trace.CorrelationIdRecordInterceptor;

/** 복구기가 market.insight 전용 팩토리에만 붙고, 기본 팩토리(market.snapshot)에는 퍼지지 않아야 한다. */
class InsightErrorHandlingWiringTest {

    @Test
    void errorHandlerIsAttachedOnlyToTheInsightFactory() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withUserConfiguration(InsightErrorHandling.class)
                .withBean(FailedEventRecoverer.class, () -> mock(FailedEventRecoverer.class))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).doesNotHaveBean(CommonErrorHandler.class);

                    var insight = context.getBean(InsightListener.CONTAINER_FACTORY, ConcurrentKafkaListenerContainerFactory.class);
                    var snapshot = context.getBean("kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(insight).isNotSameAs(snapshot);
                    assertThat(errorHandlerOf(insight)).isInstanceOf(DefaultErrorHandler.class);
                    assertThat(errorHandlerOf(snapshot)).isNull();
                    // 전용 팩토리도 처리 중 로그 줄에 correlationId 를 붙인다.
                    assertThat(org.springframework.test.util.ReflectionTestUtils.getField(insight, "recordInterceptor"))
                            .isInstanceOf(CorrelationIdRecordInterceptor.class);
                });
    }

    private static Object errorHandlerOf(ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
        return org.springframework.test.util.ReflectionTestUtils.getField(factory, "commonErrorHandler");
    }
}

package dev.stockreading.market.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;

class InsightErrorHandlingTest {

    private final Consumer<?, ?> consumer = mock(Consumer.class);
    private final MessageListenerContainer container = mock(MessageListenerContainer.class);
    private final ConsumerRecord<String, String> record = new ConsumerRecord<>("market.insight", 1, 3L, "k", "{}");
    private final Exception failure = new IllegalStateException("boom");

    private FailedEventRepository repository;
    private DefaultErrorHandler handler;

    @BeforeEach
    void setUp() {
        repository = mock(FailedEventRepository.class);
        handler = InsightErrorHandling.errorHandler(new FailedEventRecoverer(repository));
    }

    @Test
    void retriesNineTimesThenRecordsTheFailedEvent() {
        for (int retry = 1; retry <= InsightErrorHandling.RETRIES; retry++) {
            assertThat(handler.handleOne(failure, record, consumer, container)).isFalse();
        }
        verify(repository, never()).save(any());

        assertThat(handler.handleOne(failure, record, consumer, container)).isTrue();

        verify(repository).save(any(FailedEvent.class));
    }

    @Test
    void doesNotSkipTheRecordWhenRecordingItFails() {
        doThrow(new IllegalStateException("db down")).when(repository).save(any());
        for (int retry = 1; retry <= InsightErrorHandling.RETRIES; retry++) {
            handler.handleOne(failure, record, consumer, container);
        }

        // 보관에 실패하면 건너뛰었다고 보고하지 않아야 레코드가 다시 전달된다.
        assertThat(handler.handleOne(failure, record, consumer, container)).isFalse();
    }
}

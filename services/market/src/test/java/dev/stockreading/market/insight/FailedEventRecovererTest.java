package dev.stockreading.market.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class FailedEventRecovererTest {

    private FailedEventRepository repository;
    private FailedEventRecoverer recoverer;

    @BeforeEach
    void setUp() {
        repository = mock(FailedEventRepository.class);
        recoverer = new FailedEventRecoverer(repository);
    }

    @Test
    void storesTheRecordPositionKeyPayloadAndCause() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("market.insight", 2, 17L, "daily-price:TEST01", "{\"response\":\"x\"}");

        recoverer.accept(record, new IllegalStateException("처리 실패"));

        assertThat(saved()).isEqualTo(new FailedEvent(
                "market.insight", 2, 17L, "daily-price:TEST01", "{\"response\":\"x\"}",
                "java.lang.IllegalStateException", "처리 실패", 10));
    }

    @Test
    void storesTheInnermostCauseNotTheWrapper() {
        Exception wrapped = new RuntimeException("listener failed",
                new IllegalArgumentException("middle", new ArithmeticException("value too long for TEST01")));

        recoverer.accept(record(), wrapped);

        assertThat(saved().exceptionClass()).isEqualTo("java.lang.ArithmeticException");
        assertThat(saved().exceptionMessage()).isEqualTo("value too long for TEST01");
    }

    @Test
    void truncatesTheMessageToTheLimit() {
        recoverer.accept(record(), new IllegalStateException("x".repeat(1500)));

        assertThat(saved().exceptionMessage()).hasSize(FailedEventRecoverer.MAX_MESSAGE_LENGTH);
    }

    @Test
    void keepsAMissingMessageAndKeyAsNull() {
        ConsumerRecord<String, String> noKey = new ConsumerRecord<>("market.insight", 0, 1L, null, "{}");

        recoverer.accept(noKey, new IllegalStateException());

        assertThat(saved().key()).isNull();
        assertThat(saved().exceptionMessage()).isNull();
    }

    @Test
    void propagatesStorageFailureSoTheRecordIsRetriedInsteadOfLost() {
        doThrow(new IllegalStateException("db down")).when(repository).save(any());

        assertThatThrownBy(() -> recoverer.accept(record(), new RuntimeException("boom")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("db down");
    }

    private FailedEvent saved() {
        ArgumentCaptor<FailedEvent> captor = ArgumentCaptor.forClass(FailedEvent.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }

    private static ConsumerRecord<String, String> record() {
        return new ConsumerRecord<>("market.insight", 0, 5L, "k", "{}");
    }
}

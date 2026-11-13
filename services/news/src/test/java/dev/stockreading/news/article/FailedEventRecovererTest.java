package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.json.JsonMapper;

class FailedEventRecovererTest {

    private FailedEventRepository repository;
    private ArticleResultRepository results;
    private FailedEventRecoverer recoverer;

    @BeforeEach
    void setUp() {
        repository = mock(FailedEventRepository.class);
        results = mock(ArticleResultRepository.class);
        recoverer = new FailedEventRecoverer(repository, results, JsonMapper.builder().build());
    }

    @Test
    void storesTheRecordPositionKeyPayloadAndCause() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("news.collected", 2, 17L, "https://news.example.com/article/1", "{\"title\":\"테스트\"}");

        recoverer.accept(record, new IllegalStateException("처리 실패"));

        assertThat(saved()).isEqualTo(new FailedEvent(
                "news.collected", 2, 17L, "https://news.example.com/article/1", "{\"title\":\"테스트\"}",
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
        ConsumerRecord<String, String> noKey = new ConsumerRecord<>("news.collected", 0, 1L, null, "{}");

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

    // 저장 끝 알림이 이 기사를 '저장 실패'로 세려면 그 기사 이벤트의 결과가 남아야 한다.
    @Test
    void skippedArticleIsRecordedAsFailedForTheStoredNotice() {
        String eventId = "7a1f9e60-2b3c-4d5e-8f90-a1b2c3d4e5f6";
        ConsumerRecord<String, String> article = new ConsumerRecord<>("news.collected", 1, 9L, "k",
                "{\"eventId\":\"" + eventId + "\",\"correlationId\":\"collect-news-1\",\"payload\":{}}");

        recoverer.accept(article, new IllegalStateException("처리 실패"));

        verify(results).record(UUID.fromString(eventId), ArticleResult.FAILED, "collect-news-1");
    }

    @Test
    void articleWithoutAReadableEventIdIsStillKeptButNotRecorded() {
        recoverer.accept(new ConsumerRecord<>("news.collected", 0, 3L, "k", "not json"), new IllegalStateException());
        recoverer.accept(record(), new IllegalStateException());

        verifyNoInteractions(results);
        verify(repository, times(2)).save(any());
    }

    @Test
    void recordsFromOtherTopicsAreNotArticleResults() {
        recoverer.accept(new ConsumerRecord<>("filing.collected", 0, 3L, "k",
                "{\"eventId\":\"7a1f9e60-2b3c-4d5e-8f90-a1b2c3d4e5f6\"}"), new IllegalStateException());

        verifyNoInteractions(results);
    }

    private FailedEvent saved() {
        ArgumentCaptor<FailedEvent> captor = ArgumentCaptor.forClass(FailedEvent.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }

    private static ConsumerRecord<String, String> record() {
        return new ConsumerRecord<>("news.collected", 0, 5L, "k", "{}");
    }
}

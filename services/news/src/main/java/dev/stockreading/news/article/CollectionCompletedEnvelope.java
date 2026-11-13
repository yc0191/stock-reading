package dev.stockreading.news.article;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** collection.completed 의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record CollectionCompletedEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        CompletedPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record CompletedPayload(
            String jobType,
            String scheduledFor,
            String startedAt,
            String finishedAt,
            int publishedCount,
            List<String> articleEventIds,
            List<String> incompleteSymbols,
            String researchJobId) {
    }
}

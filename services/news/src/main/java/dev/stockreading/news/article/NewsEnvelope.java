package dev.stockreading.news.article;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** news.collected의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record NewsEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        NewsPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record NewsPayload(
            String title,
            String summary,
            String url,
            String publisher,
            String publishedAt,
            List<String> symbols,
            List<String> matchedQueries,
            String researchJobId) {
    }
}

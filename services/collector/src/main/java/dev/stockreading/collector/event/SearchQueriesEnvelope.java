package dev.stockreading.collector.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** news.search-queries 의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SearchQueriesEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        SearchQueriesPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SearchQueriesPayload(String contentHash, List<Query> queries) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Query(String query, List<String> symbols) {
    }
}

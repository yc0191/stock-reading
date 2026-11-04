package dev.stockreading.collector.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** news.research.requested 의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ResearchRequestEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        ResearchRequestPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ResearchRequestPayload(String jobId, int seq, String jobKind, String symbol, List<String> queries) {
    }
}

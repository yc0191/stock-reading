package dev.stockreading.market.insight;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** market.insight의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record InsightEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        InsightPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record InsightPayload(
            String apiType,
            String target,
            String response,
            String scheduledFor,
            String fetchedAt) {
    }
}

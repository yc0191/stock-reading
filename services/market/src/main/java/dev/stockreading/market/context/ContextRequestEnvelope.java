package dev.stockreading.market.context;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** market.context.requested의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record ContextRequestEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        RequestPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record RequestPayload(
            String jobId, int seq, String symbol, String asOf, String since, int maxResponseBytes, int maxTickers) {
    }
}

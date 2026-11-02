package dev.stockreading.market.history;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** market.history.requested의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record HistoryRequestEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        RequestPayload payload) {

    /** intervalSeconds는 선택 필드다. 이전 형식 요청에는 없어서 null이다. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record RequestPayload(
            String jobId, int seq, String symbol, String since, String until, int maxPoints, Integer intervalSeconds) {
    }
}

package dev.stockreading.market.snapshot;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** market.snapshot.failed의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FailureEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        FailurePayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FailurePayload(
            String symbol,
            String failureType,
            String scheduledFor,
            Integer httpStatus,
            String responseCode,
            Long elapsedMs,
            String exceptionType,
            TokenState tokenState,
            Integer concurrentCalls,
            Integer tickSucceeded,
            Integer tickFailed,
            String responseRaw) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TokenState(Long ageMs, Long remainingMs) {
    }
}

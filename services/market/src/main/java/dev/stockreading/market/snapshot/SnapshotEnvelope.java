package dev.stockreading.market.snapshot;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** market.snapshot의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SnapshotEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        SnapshotPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record SnapshotPayload(String symbol, long price, String asOf, String source) {
    }
}

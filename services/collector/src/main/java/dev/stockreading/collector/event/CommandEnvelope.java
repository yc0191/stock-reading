package dev.stockreading.collector.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** collector.command 의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CommandEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        CommandPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record CommandPayload(String command) {
    }
}

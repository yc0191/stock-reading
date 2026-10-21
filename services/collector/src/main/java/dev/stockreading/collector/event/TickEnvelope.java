package dev.stockreading.collector.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** schedule.tick의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TickEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        TickPayload payload) {
}

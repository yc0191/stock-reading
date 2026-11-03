package dev.stockreading.notifier.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** briefing.daily.generated의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DailyBriefingEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        Payload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(
            String edition,
            String rangeStart,
            String rangeEnd,
            String text,
            List<String> missingItems,
            String generatedBy) {
    }
}

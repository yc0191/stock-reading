package dev.stockreading.notifier.event;

import java.math.BigDecimal;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** briefing.generated의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BriefingEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        Payload payload) {

    /** todayChangePercent, moveMinutes는 짧은 시간 변동일 때만 오는 선택 칸이다. 없으면 null이다. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(
            String symbol,
            String name,
            BigDecimal changePercent,
            String asOf,
            String text,
            String generatedBy,
            List<Evidence> evidence,
            BigDecimal todayChangePercent,
            Integer moveMinutes) {

        public Payload(String symbol, String name, BigDecimal changePercent, String asOf, String text,
                String generatedBy, List<Evidence> evidence) {
            this(symbol, name, changePercent, asOf, text, generatedBy, evidence, null, null);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Evidence(String title, String source, String publishedAt) {
    }
}

package dev.stockreading.collector.event;

/** 임시 공통 봉투. 최종 필드 구성은 구현 단계에서 정한다(ADR-010 초안). */
public record Envelope<T>(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        T payload) {
}

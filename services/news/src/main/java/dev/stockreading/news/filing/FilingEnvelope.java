package dev.stockreading.news.filing;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** filing.collected의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record FilingEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        FilingPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record FilingPayload(
            List<String> tickers,
            String cik,
            String accessionNumber,
            String form,
            List<String> items,
            String acceptedAt,
            String filingDate,
            String url) {
    }
}

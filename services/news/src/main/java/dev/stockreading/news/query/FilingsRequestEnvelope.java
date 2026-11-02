package dev.stockreading.news.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** news.filings.requested의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record FilingsRequestEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        RequestPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record RequestPayload(
            String jobId, int seq, String symbol, List<String> tickers, String since, String until, int maxFilings) {
    }
}

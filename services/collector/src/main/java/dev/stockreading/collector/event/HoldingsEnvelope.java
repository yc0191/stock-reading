package dev.stockreading.collector.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** market.holdings 의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record HoldingsEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        HoldingsPayload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record HoldingsPayload(String contentHash, List<Instrument> instruments, List<Membership> memberships) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Instrument(String kind, String market, String symbol, String relation) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Membership(String etfMarket, String etfSymbol, String memberMarket, String memberSymbol,
            Integer rank) {
    }
}

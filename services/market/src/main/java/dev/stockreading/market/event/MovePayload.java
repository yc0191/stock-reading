package dev.stockreading.market.event;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonInclude;

/** detectionType, moveStartedAt은 선택 필드다. null이면 직렬화에서 빠져 이전 형식과 같아진다. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MovePayload(
        String symbol,
        long previousPrice,
        long price,
        BigDecimal changePercent,
        BigDecimal thresholdPercent,
        String asOf,
        String detectionType,
        String moveStartedAt) {

    public MovePayload(
            String symbol, long previousPrice, long price, BigDecimal changePercent,
            BigDecimal thresholdPercent, String asOf) {
        this(symbol, previousPrice, price, changePercent, thresholdPercent, asOf, null, null);
    }
}

package dev.stockreading.market.event;

import java.math.BigDecimal;

public record MovePayload(
        String symbol,
        long previousPrice,
        long price,
        BigDecimal changePercent,
        BigDecimal thresholdPercent,
        String asOf) {
}

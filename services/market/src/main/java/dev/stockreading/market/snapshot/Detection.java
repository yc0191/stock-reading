package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/** 감지된 변동 하나. moveStartedAt은 감지 규칙이 정한 변동 시각이다. */
record Detection(
        DetectionType type,
        long previousPrice,
        long price,
        BigDecimal changePercent,
        BigDecimal thresholdPercent,
        OffsetDateTime moveStartedAt) {
}

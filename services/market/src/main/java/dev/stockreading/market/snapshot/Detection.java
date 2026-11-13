package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

/**
 * 감지된 변동 하나. moveStartedAt은 감지 규칙이 정한 변동 시각이다.
 * todayChangePercent는 감지한 스냅샷 원문의 전일 종가 대비 변동률이며, 전일 종가를 정하지 못하면 null이다.
 */
record Detection(
        DetectionType type,
        long previousPrice,
        long price,
        BigDecimal changePercent,
        BigDecimal thresholdPercent,
        OffsetDateTime moveStartedAt,
        BigDecimal todayChangePercent) {

    Detection(DetectionType type, long previousPrice, long price, BigDecimal changePercent,
            BigDecimal thresholdPercent, OffsetDateTime moveStartedAt) {
        this(type, previousPrice, price, changePercent, thresholdPercent, moveStartedAt, null);
    }
}

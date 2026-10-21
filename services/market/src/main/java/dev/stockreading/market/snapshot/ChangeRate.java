package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.math.RoundingMode;

/** 두 가격의 변동률(%). LLM이 아니라 코드가 계산한다(ADR-007). */
public final class ChangeRate {

    private static final BigDecimal HUNDRED = BigDecimal.valueOf(100);

    private ChangeRate() {
    }

    /** @return (current - previous) / previous * 100, 소수점 2자리 반올림(HALF_UP). */
    public static BigDecimal percent(long previous, long current) {
        if (previous <= 0) {
            throw new IllegalArgumentException("previous price must be positive: " + previous);
        }
        return BigDecimal.valueOf(current - previous)
                .multiply(HUNDRED)
                .divide(BigDecimal.valueOf(previous), 2, RoundingMode.HALF_UP);
    }
}

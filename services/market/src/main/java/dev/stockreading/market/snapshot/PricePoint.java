package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;

/** 저장된 스냅샷의 시각과 가격. beforeFirstTrade는 원문의 첫 체결 전 칸이 모두 0인 응답이라는 뜻이다. */
record PricePoint(OffsetDateTime asOf, long price, boolean beforeFirstTrade) {

    PricePoint(OffsetDateTime asOf, long price) {
        this(asOf, price, false);
    }
}

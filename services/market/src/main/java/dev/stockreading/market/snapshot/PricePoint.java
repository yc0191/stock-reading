package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;

/** 저장된 스냅샷의 시각과 가격. */
record PricePoint(OffsetDateTime asOf, long price) {
}

package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;

/** 종목 단위로 유형과 상관없이 마지막으로 market.move를 낸 가격과 시각(변동을 감지한 스냅샷의 시각). */
record LastPublish(long price, OffsetDateTime publishedAt) {
}

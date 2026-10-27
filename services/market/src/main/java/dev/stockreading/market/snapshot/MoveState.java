package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;

/** 종목·유형별로 마지막으로 발행한 시점의 가격과, 그 변동이 시작된 시각. */
record MoveState(long lastPublishedPrice, OffsetDateTime moveStartedAt) {
}

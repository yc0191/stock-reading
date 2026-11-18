package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;

/**
 * 종목·유형별로 마지막으로 발행한(간격 때문에 막은 것 포함) 시점의 가격과 시각, 그 변동이 시작된 시각.
 * 감지 조건이 풀리면 시작 시각 칸에 풀린 시각을 넣어 끝난 변동으로 표시한다(MoveStateRepository.markEnded).
 */
record MoveState(long lastPublishedPrice, OffsetDateTime moveStartedAt, OffsetDateTime lastPublishedAt) {

    /** 끝난 변동이면 가격은 그날 재발행 판단에 계속 쓰고, 다음 발행은 새 변동으로 시작 시각을 새로 잡는다. */
    boolean ended() {
        return moveStartedAt.isAfter(lastPublishedAt);
    }
}

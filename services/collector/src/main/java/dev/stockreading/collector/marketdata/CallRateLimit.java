package dev.stockreading.collector.marketdata;

import java.time.Duration;

/**
 * 외부 시세 API 호출 상한의 접점. 지금은 인스턴스 안의 메모리 구현({@link CallRateLimiter})만 있다.
 * 제공자 한도는 앱 전체와 API별로 걸리므로, 다른 조회가 늘거나 인스턴스가 늘면 Redis 카운터 구현으로 바꿔 끼운다(ADR-008).
 */
interface CallRateLimit {

    /** 자리가 나길 maxWait 안에서만 기다린다. 얻지 못하면 false 이고 호출하면 안 된다. */
    boolean tryAcquire(Duration maxWait);
}

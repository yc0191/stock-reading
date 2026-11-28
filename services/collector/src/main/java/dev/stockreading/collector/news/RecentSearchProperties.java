package dev.stockreading.collector.news;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

// window 는 같은 검색어의 결과를 다시 쓰는 기간(정기 수집 주기와 같게 둔다), waitLimit 은 다른 쪽이 검색 중인
// 결과를 기다리는 상한, lockTtl 은 검색 중 표시가 저절로 풀리는 시간, pollInterval 은 기다리는 동안 다시 읽는 간격이다.
// 잘못된 값은 시작 단계에서 막는다.
@ConfigurationProperties("collector.news-reuse")
public record RecentSearchProperties(Duration window, Duration waitLimit, Duration lockTtl, Duration pollInterval) {

    public RecentSearchProperties {
        if (window == null || window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("collector.news-reuse.window 는 0보다 커야 합니다");
        }
        if (waitLimit == null || waitLimit.isNegative()) {
            throw new IllegalArgumentException("collector.news-reuse.wait-limit 는 0 이상이어야 합니다");
        }
        if (lockTtl == null || lockTtl.isNegative() || lockTtl.isZero()) {
            throw new IllegalArgumentException("collector.news-reuse.lock-ttl 은 0보다 커야 합니다");
        }
        if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("collector.news-reuse.poll-interval 은 0보다 커야 합니다");
        }
    }
}

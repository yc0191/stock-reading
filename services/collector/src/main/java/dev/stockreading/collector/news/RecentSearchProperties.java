package dev.stockreading.collector.news;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

// window 는 같은 검색어의 결과를 다시 쓰는 기간(정기 수집 주기와 같게 둔다), waitLimit 은 다른 쪽이 검색 중인
// 결과를 기다리는 상한, maxEntries 는 메모리에 남길 검색어 수 상한이다. 잘못된 값은 시작 단계에서 막는다.
@ConfigurationProperties("collector.news-reuse")
public record RecentSearchProperties(Duration window, Duration waitLimit, int maxEntries) {

    public RecentSearchProperties {
        if (window == null || window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("collector.news-reuse.window 는 0보다 커야 합니다");
        }
        if (waitLimit == null || waitLimit.isNegative()) {
            throw new IllegalArgumentException("collector.news-reuse.wait-limit 는 0 이상이어야 합니다");
        }
        if (maxEntries < 1) {
            throw new IllegalArgumentException("collector.news-reuse.max-entries 는 1 이상이어야 합니다");
        }
    }
}

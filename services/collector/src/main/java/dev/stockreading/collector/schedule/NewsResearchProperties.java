package dev.stockreading.collector.schedule;

import org.springframework.boot.context.properties.ConfigurationProperties;

// maxQueries 는 재검색 요청 하나에서 실제로 검색할 검색어 수의 상한이다. 요청을 만드는 쪽(브리핑)에도 상한이 있지만,
// 검색어 하나가 하루 호출 상한을 한 번씩 쓰므로 수집기도 따로 막는다. 0 이하는 설정 오류로 시작 단계에서 막는다.
@ConfigurationProperties("collector.news-research")
public record NewsResearchProperties(int maxQueries) {

    public NewsResearchProperties {
        if (maxQueries < 1) {
            throw new IllegalArgumentException("collector.news-research.max-queries 는 1 이상이어야 합니다");
        }
    }
}

package dev.stockreading.collector.news;

import java.time.OffsetDateTime;
import java.util.LinkedHashSet;
import java.util.Set;

// 본문은 가져오지 않는다(API 가 제목과 요약만 준다). publishedAt 은 제공자가 알려 준 시각이라
// 원 기사의 발행 시각과 다를 수 있다.
public record NewsItem(
        String title,
        String summary,
        String url,
        String providerUrl,
        OffsetDateTime publishedAt,
        String source,
        Set<String> symbols,
        Set<String> matchedQueries) {

    // 같은 기사가 다른 검색어로도 걸렸을 때 합친다. 종목·검색어는 합집합, 시각은 더 이른 쪽을 쓴다.
    NewsItem mergedWith(NewsItem other) {
        Set<String> mergedSymbols = new LinkedHashSet<>(symbols);
        mergedSymbols.addAll(other.symbols);
        Set<String> mergedQueries = new LinkedHashSet<>(matchedQueries);
        mergedQueries.addAll(other.matchedQueries);
        OffsetDateTime earlier = other.publishedAt.isBefore(publishedAt) ? other.publishedAt : publishedAt;
        return new NewsItem(title, summary, url, providerUrl, earlier, source, mergedSymbols, mergedQueries);
    }
}

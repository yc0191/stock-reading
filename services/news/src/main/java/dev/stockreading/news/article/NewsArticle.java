package dev.stockreading.news.article;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * 저장할 기사 한 건. 본문은 저장하지 않으므로 제목과 요약만 가진다.
 * researchJobId 는 재검색에서 찾은 기사에만 있고, 정기 수집 기사는 null 이다.
 */
record NewsArticle(
        UUID eventId,
        String title,
        String summary,
        String url,
        String publisher,
        OffsetDateTime publishedAt,
        List<String> symbols,
        List<String> matchedQueries,
        UUID researchJobId) {

    NewsArticle(UUID eventId, String title, String summary, String url, String publisher, OffsetDateTime publishedAt,
            List<String> symbols, List<String> matchedQueries) {
        this(eventId, title, summary, url, publisher, publishedAt, symbols, matchedQueries, null);
    }
}

package dev.stockreading.news.article;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** 저장할 기사 한 건. 본문은 저장하지 않으므로 제목과 요약만 가진다. */
record NewsArticle(
        UUID eventId,
        String title,
        String summary,
        String url,
        String publisher,
        OffsetDateTime publishedAt,
        List<String> symbols,
        List<String> matchedQueries) {
}

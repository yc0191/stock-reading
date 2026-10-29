package dev.stockreading.news.event;

import java.util.List;

/** news.articles.responded의 payload. jobId와 seq는 요청 값을 그대로 되돌려준다. */
public record ArticlesPayload(String jobId, int seq, String symbol, List<Article> articles, boolean truncated) {

    public record Article(
            String articleId,
            String title,
            String summary,
            String publishedAt,
            String publisher,
            String url) {
    }
}

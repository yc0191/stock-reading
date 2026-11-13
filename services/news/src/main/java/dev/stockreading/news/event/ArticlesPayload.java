package dev.stockreading.news.event;

import java.util.List;

/** 내부 조회 API 의 응답 본문(옛 news.articles 응답 이벤트의 payload 와 같은 모양). jobId와 seq는 요청 값을 그대로 되돌려준다. */
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

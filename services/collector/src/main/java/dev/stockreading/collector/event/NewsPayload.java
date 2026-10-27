package dev.stockreading.collector.event;

import java.util.List;

/** news.collected 의 payload. 본문과 개인 정보는 담지 않는다. */
public record NewsPayload(
        String title,
        String summary,
        String url,
        String publisher,
        String publishedAt,
        List<String> symbols,
        List<String> matchedQueries) {
}

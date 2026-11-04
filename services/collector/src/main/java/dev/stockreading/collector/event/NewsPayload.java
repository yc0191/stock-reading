package dev.stockreading.collector.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * news.collected 의 payload. 본문과 개인 정보는 담지 않는다.
 * researchJobId 는 재검색에서 찾은 기사에만 있는 선택 필드라, 없으면 직렬화에서 빠져야 스키마(type: string)를 지킨다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record NewsPayload(
        String title,
        String summary,
        String url,
        String publisher,
        String publishedAt,
        List<String> symbols,
        List<String> matchedQueries,
        String researchJobId) {

    public NewsPayload(String title, String summary, String url, String publisher, String publishedAt,
            List<String> symbols, List<String> matchedQueries) {
        this(title, summary, url, publisher, publishedAt, symbols, matchedQueries, null);
    }
}

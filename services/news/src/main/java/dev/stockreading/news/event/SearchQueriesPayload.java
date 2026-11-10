package dev.stockreading.news.event;

import java.util.List;

/**
 * news.search-queries v1의 payload. 검색어 테이블의 그때 전체 목록이다.
 * contentHash는 목록 내용이 같으면 같은 값이라 소비자가 바뀌었는지 비교할 수 있다.
 */
public record SearchQueriesPayload(String contentHash, List<Query> queries) {

    /** symbols는 검색어로 찾은 기사에 태그로 붙일 종목들이다. "*"는 시장 전체 이슈다. */
    public record Query(String query, List<String> symbols) {
    }
}

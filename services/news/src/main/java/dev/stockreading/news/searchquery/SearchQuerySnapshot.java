package dev.stockreading.news.searchquery;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;

import dev.stockreading.news.event.SearchQueriesPayload;

/**
 * 검색어 테이블에서 읽은 그때의 전체 목록. 읽은 순서와 상관없이 같은 내용이면 같은 해시가 나오도록
 * 검색어와 종목을 정렬해 담는다.
 */
record SearchQuerySnapshot(List<SearchQueriesPayload.Query> queries) {

    // 항목 안의 값에 구분자가 들어 있어도 다른 목록과 같은 문자열이 되지 않도록 제어 문자로 나눈다.
    private static final char FIELD = '\u001f';
    private static final char LINE = '\u001e';

    SearchQuerySnapshot {
        queries = queries.stream()
                .map(q -> new SearchQueriesPayload.Query(q.query(), q.symbols().stream().sorted().toList()))
                .sorted(Comparator.comparing(SearchQueriesPayload.Query::query))
                .toList();
    }

    String contentHash() {
        StringBuilder canonical = new StringBuilder();
        for (SearchQueriesPayload.Query q : queries) {
            canonical.append(q.query());
            for (String symbol : q.symbols()) {
                canonical.append(FIELD).append(symbol);
            }
            canonical.append(LINE);
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    SearchQueriesPayload toPayload() {
        return new SearchQueriesPayload(contentHash(), queries);
    }
}

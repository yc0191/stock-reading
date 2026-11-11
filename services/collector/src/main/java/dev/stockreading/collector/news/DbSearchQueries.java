package dev.stockreading.collector.news;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * 뉴스 검색어를 DB의 검색 대상 뷰(market.news_search_target)에서 읽기 전용으로 읽는다. 시작할 때 한 번 읽고 메모리에 두며,
 * 바뀐 값은 재시작해야 반영된다. 같은 검색어가 여러 줄이면 태그를 모아 한 검색어로 만든다.
 */
@Component
class DbSearchQueries implements SearchQueriesPort {

    private static final Logger log = LoggerFactory.getLogger(DbSearchQueries.class);

    // news.collected 의 종목코드 형식이다. 이 형식이 아닌 태그가 붙은 기사는 발행 단계에서 통째로 버려지므로
    // 읽는 단계에서 걸러 낸다.
    private static final Pattern PUBLISHABLE_SYMBOL = Pattern.compile("\\*|[A-Z0-9]{1,6}([.-][A-Z0-9]{1,2})?");

    /** 뷰의 한 줄: 이 검색어로 찾은 기사에 이 태그를 단다. */
    record Row(String query, String tag) {
    }

    private final List<NewsQuery> queries;

    DbSearchQueries(JdbcClient jdbc) {
        List<Row> rows = jdbc.sql("SELECT query, tag FROM market.news_search_target ORDER BY query, tag")
                .query((rs, rowNum) -> new Row(rs.getString("query"), rs.getString("tag"))).list();
        this.queries = group(rows);
        if (queries.isEmpty()) {
            log.warn("뉴스 검색어가 비어 있습니다(market.news_search_target). 값을 넣은 뒤 재시작해야 합니다");
        }
    }

    @Override
    public List<NewsQuery> queries() {
        return queries;
    }

    static List<NewsQuery> group(List<Row> rows) {
        Map<String, Set<String>> symbolsByQuery = new LinkedHashMap<>();
        int droppedTags = 0;
        int droppedQueries = 0;
        for (Row row : rows) {
            if (row.query() == null || row.query().isBlank()) {
                droppedQueries++;
                continue;
            }
            Set<String> symbols = symbolsByQuery.computeIfAbsent(row.query().trim(), k -> new LinkedHashSet<>());
            if (row.tag() != null && PUBLISHABLE_SYMBOL.matcher(row.tag()).matches()) {
                symbols.add(row.tag());
            } else {
                droppedTags++;
            }
        }
        List<NewsQuery> result = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : symbolsByQuery.entrySet()) {
            if (entry.getValue().isEmpty()) {
                droppedQueries++;
            } else {
                result.add(new NewsQuery(entry.getKey(), entry.getValue()));
            }
        }
        // 검색어와 태그는 관심 종목이 드러나므로 개수만 남긴다.
        log.info("뉴스 검색어를 DB에서 읽었습니다: 검색어 {}개, 제외한 검색어 {}개, 제외한 태그 {}개",
                result.size(), droppedQueries, droppedTags);
        return List.copyOf(result);
    }
}

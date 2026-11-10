package dev.stockreading.collector.news;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.event.SearchQueriesEnvelope.Query;
import dev.stockreading.collector.event.SearchQueriesEnvelope.SearchQueriesPayload;

/**
 * news.search-queries 로 받은 가장 최근의 검색어 목록. 이벤트는 그때의 전체 목록이라 같은 이벤트를 다시 받아도
 * 결과가 같다.
 */
@Component
class ReceivedSearchQueries implements SearchQueriesPort {

    private static final Logger log = LoggerFactory.getLogger(ReceivedSearchQueries.class);

    // news.collected 의 종목코드 형식이다. 이 형식이 아닌 태그가 붙은 기사는 발행 단계에서 통째로 버려지므로
    // 받는 단계에서 걸러 낸다. 미국 티커 태그는 뉴스 쪽 태그 칸이 넓어진 뒤에 쓸 수 있다.
    private static final Pattern PUBLISHABLE_SYMBOL = Pattern.compile("[0-9A-Z]{6}|\\*");

    private volatile List<NewsQuery> queries;

    @Override
    public List<NewsQuery> queries() {
        List<NewsQuery> current = queries;
        if (current == null) {
            log.warn("검색어 목록을 아직 받지 못했습니다(news.search-queries). 뉴스 수집을 건너뜁니다");
            throw new IllegalStateException("검색어 목록을 아직 받지 못했습니다");
        }
        return current;
    }

    void update(SearchQueriesPayload payload) {
        List<NewsQuery> received = new ArrayList<>();
        int droppedSymbols = 0;
        int droppedQueries = 0;
        for (Query q : payload.queries()) {
            Set<String> symbols = new LinkedHashSet<>();
            for (String symbol : q.symbols() == null ? List.<String>of() : q.symbols()) {
                if (symbol != null && PUBLISHABLE_SYMBOL.matcher(symbol).matches()) {
                    symbols.add(symbol);
                } else {
                    droppedSymbols++;
                }
            }
            if (q.query() == null || q.query().isBlank() || symbols.isEmpty()) {
                droppedQueries++;
                continue;
            }
            received.add(new NewsQuery(q.query().trim(), symbols));
        }
        this.queries = List.copyOf(received);
        // 검색어와 종목은 관심 종목이 드러나므로 개수만 남긴다.
        log.info("검색어 목록을 받았습니다: 검색어 {}개, 제외한 검색어 {}개, 제외한 태그 {}개",
                received.size(), droppedQueries, droppedSymbols);
    }
}

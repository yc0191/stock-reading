package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

class NewsQueryTest {

    @Test
    void parsesQueriesWithSymbolsAndMarketWide() {
        List<NewsQuery> queries = NewsQuery.parse("테스트검색어=TEST01,TEST02; 테스트 시황 = * ");

        assertThat(queries).extracting(NewsQuery::query).containsExactly("테스트검색어", "테스트 시황");
        assertThat(queries.get(0).symbols()).containsExactly("TEST01", "TEST02");
        assertThat(queries.get(1).symbols()).containsExactly(NewsQuery.MARKET_WIDE);
    }

    @Test
    void skipsMalformedEntries() {
        assertThat(NewsQuery.parse("noequals;=TEST01;empty=;ok=A")).extracting(NewsQuery::query).containsExactly("ok");
    }

    @Test
    void blankSpecGivesNoQueries() {
        assertThat(NewsQuery.parse(null)).isEmpty();
        assertThat(NewsQuery.parse("  ")).isEmpty();
    }
}

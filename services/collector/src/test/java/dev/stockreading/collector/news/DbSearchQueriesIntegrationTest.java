package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;

import dev.stockreading.collector.CollectorDb;

/**
 * 뉴스 검색어를 수집기 계정으로 검색 대상 뷰(market.news_search_target)에서 읽는 것을 실제 Postgres로 확인한다.
 * 가짜 종목·검색어(COLL 접두어)를 시세처리기 계정으로 넣는다.
 */
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class DbSearchQueriesIntegrationTest {

    private final JdbcClient market = CollectorDb.market();

    @BeforeEach
    void seed() {
        CollectorDb.reset();
        market.sql("INSERT INTO etf (market, symbol, name, search_by_name) VALUES ('KR', 'COLL01', 'COLL ETF ONE', TRUE)").update();
        market.sql("INSERT INTO stock (market, symbol, name, search_by_name) VALUES ('US', 'COLLA', 'COLL Corp A', TRUE), "
                + "('US', 'COLLB', 'COLL Corp B', FALSE)").update();
        keyword("COLL 검색어", true);
        keyword("COLL 꺼진 검색어", false);
        keyword("COLL 환율 뉴스", true);
        keyword("COLL 금리 뉴스", true);
        market.sql("INSERT INTO macro_keyword (name) VALUES ('COLL 환율'), ('COLL 금리')").update();
        map("COLL 검색어", "etf_id", "SELECT id FROM etf WHERE symbol = 'COLL01'");
        map("COLL 검색어", "stock_id", "SELECT id FROM stock WHERE symbol = 'COLLB'");
        map("COLL 꺼진 검색어", "etf_id", "SELECT id FROM etf WHERE symbol = 'COLL01'");
        map("COLL 환율 뉴스", "macro_keyword_id", "SELECT id FROM macro_keyword WHERE name = 'COLL 환율'");
        map("COLL 금리 뉴스", "macro_keyword_id", "SELECT id FROM macro_keyword WHERE name = 'COLL 금리'");
        // 환율 주제만 종목에 연결하고, 연결이 없는 금리 주제의 검색어는 시장 전체(*) 태그가 된다.
        market.sql("INSERT INTO macro_keyword_map (macro_keyword_id, etf_id) SELECT m.id, e.id FROM macro_keyword m, etf e "
                + "WHERE m.name = 'COLL 환율' AND e.symbol = 'COLL01'").update();
    }

    private void keyword(String keyword, boolean enabled) {
        market.sql("INSERT INTO news_keyword (keyword, enabled) VALUES (:k, :e)").param("k", keyword).param("e", enabled).update();
    }

    private void map(String keyword, String column, String idQuery) {
        market.sql("INSERT INTO news_keyword_map (keyword_id, " + column + ") SELECT k.id, (" + idQuery
                + ") FROM news_keyword k WHERE k.keyword = :k").param("k", keyword).update();
    }

    private static NewsQuery find(List<NewsQuery> queries, String query) {
        return queries.stream().filter(q -> q.query().equals(query)).findFirst().orElseThrow();
    }

    @Test
    void theCollectorAccountReadsNameQueriesAndKeywordQueriesWithTheirTags() {
        List<NewsQuery> queries = new DbSearchQueries(CollectorDb.collector()).queries();

        // 종목 이름 자동 검색(search_by_name)은 그 종목 코드를 태그로 단다. 꺼 둔 종목(COLLB)은 이름으로 찾지 않는다.
        assertThat(find(queries, "COLL ETF ONE").symbols()).containsExactly("COLL01");
        assertThat(find(queries, "COLL Corp A").symbols()).containsExactly("COLLA");
        assertThat(queries).extracting(NewsQuery::query).doesNotContain("COLL Corp B");
        // 한 검색어가 ETF와 주식에 함께 걸리면 태그를 모은다.
        assertThat(find(queries, "COLL 검색어").symbols()).containsExactlyInAnyOrder("COLL01", "COLLB");
    }

    @Test
    void disabledKeywordsAreLeftOutAndMacroKeywordsTagLinkedStocksOrTheWholeMarket() {
        List<NewsQuery> queries = new DbSearchQueries(CollectorDb.collector()).queries();

        assertThat(queries).extracting(NewsQuery::query).doesNotContain("COLL 꺼진 검색어");
        assertThat(find(queries, "COLL 환율 뉴스").symbols()).containsExactly("COLL01");
        assertThat(find(queries, "COLL 금리 뉴스").symbols()).containsExactly("*");
    }
}

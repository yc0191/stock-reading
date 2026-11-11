package dev.stockreading.market.constituent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import dev.stockreading.market.DisposableDatabase;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 뉴스 검색어 표와 검색 대상 뷰(news_search_target)의 태그 규칙을 실제 Postgres로 확인한다:
 * 이름 자동 검색, 검색어→ETF·주식, 거시 주제→연결된 종목 또는 시장 전체(*), 꺼진 검색어와 주제 제외, 표 규칙(정확히 하나).
 * personal 표와 겹치지 않도록 이 클래스는 검색어 표와 NEWS 접두어 종목만 쓴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
class NewsSearchTargetIntegrationTest {

    private static JdbcClient jdbc;
    private static JdbcClient writer;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");
        DisposableDatabase.require(JdbcClient.create(new DriverManagerDataSource(url, user, password)));
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url, user, password));
        writer = JdbcClient.create(new DriverManagerDataSource(url, System.getenv("WRITER_IT_DB_USER"),
                System.getenv("WRITER_IT_DB_PASSWORD")));
    }

    @BeforeEach
    void startEmpty() {
        writer.sql("DELETE FROM personal.member_portfolio").update();
        writer.sql("DELETE FROM personal.member").update();
        jdbc.sql("DELETE FROM news_keyword_map").update();
        jdbc.sql("DELETE FROM macro_keyword_map").update();
        jdbc.sql("DELETE FROM news_keyword").update();
        jdbc.sql("DELETE FROM macro_keyword").update();
        jdbc.sql("DELETE FROM etf_stock_map").update();
        jdbc.sql("DELETE FROM etf").update();
        jdbc.sql("DELETE FROM stock").update();
        jdbc.sql("INSERT INTO etf (market, symbol, name, search_by_name) VALUES ('KR', 'NEWE01', 'NEWS ETF ONE', TRUE), "
                + "('KR', 'NEWE02', 'NEWS ETF TWO', FALSE)").update();
        jdbc.sql("INSERT INTO stock (market, symbol, name, search_by_name) VALUES ('US', 'NEWSA', 'NEWS CORP A', TRUE), "
                + "('US', 'NEWSB', 'NEWS CORP B', FALSE), ('US', 'NEWSC', NULL, TRUE)").update();
    }

    private static long keyword(String text, boolean enabled) {
        return jdbc.sql("INSERT INTO news_keyword (keyword, enabled) VALUES (:k, :e) RETURNING id")
                .param("k", text).param("e", enabled).query(Long.class).single();
    }

    private static long macro(String name, boolean enabled) {
        return jdbc.sql("INSERT INTO macro_keyword (name, enabled) VALUES (:n, :e) RETURNING id")
                .param("n", name).param("e", enabled).query(Long.class).single();
    }

    private static void mapKeyword(long keywordId, String column, Object targetId) {
        jdbc.sql("INSERT INTO news_keyword_map (keyword_id, " + column + ") VALUES (:k, :t)")
                .param("k", keywordId).param("t", targetId).update();
    }

    private static Object etfId(String symbol) {
        return jdbc.sql("SELECT id FROM etf WHERE symbol = :s").param("s", symbol).query(Long.class).single();
    }

    private static Object stockId(String symbol) {
        return jdbc.sql("SELECT id FROM stock WHERE symbol = :s").param("s", symbol).query(Long.class).single();
    }

    private static List<String> targets() {
        return jdbc.sql("SELECT query || '>' || tag || ':' || kind || ':' || origin FROM news_search_target ORDER BY 1")
                .query(String.class).list();
    }

    @Test
    void namesAreSearchedOnlyForTagsWithSearchByNameAndANonEmptyName() {
        assertThat(targets()).containsExactly("NEWS CORP A>NEWSA:STOCK:NAME", "NEWS ETF ONE>NEWE01:ETF:NAME");
    }

    @Test
    void aKeywordOfAnEtfOrStockTagsThatSymbolAndATargetCanHaveSeveralKeywords() {
        long first = keyword("kw one", true);
        long second = keyword("kw two", true);
        mapKeyword(first, "etf_id", etfId("NEWE02"));
        mapKeyword(second, "etf_id", etfId("NEWE02"));
        mapKeyword(first, "stock_id", stockId("NEWSB"));

        assertThat(targets()).contains("kw one>NEWE02:ETF:KEYWORD", "kw two>NEWE02:ETF:KEYWORD",
                "kw one>NEWSB:STOCK:KEYWORD");
    }

    @Test
    void aMacroKeywordTagsTheConnectedSymbolsOrTheWholeMarketWhenNothingIsConnected() {
        long connected = macro("MACRO CONNECTED", true);
        long free = macro("MACRO FREE", true);
        long off = macro("MACRO OFF", false);
        jdbc.sql("INSERT INTO macro_keyword_map (macro_keyword_id, etf_id) VALUES (:m, :e)")
                .param("m", connected).param("e", etfId("NEWE02")).update();
        jdbc.sql("INSERT INTO macro_keyword_map (macro_keyword_id, stock_id) VALUES (:m, :s)")
                .param("m", connected).param("s", stockId("NEWSB")).update();
        mapKeyword(keyword("rate news", true), "macro_keyword_id", connected);
        mapKeyword(keyword("fx news", true), "macro_keyword_id", free);
        mapKeyword(keyword("off news", true), "macro_keyword_id", off);

        List<String> keywordRows = targets().stream().filter(t -> t.endsWith(":KEYWORD")).toList();

        assertThat(keywordRows).containsExactlyInAnyOrder("rate news>NEWE02:ETF:KEYWORD",
                "rate news>NEWSB:STOCK:KEYWORD", "fx news>*:MARKET:KEYWORD");
    }

    @Test
    void disabledKeywordsAreLeftOut() {
        long off = keyword("kw off", false);
        mapKeyword(off, "etf_id", etfId("NEWE02"));
        long macroOn = macro("MACRO ON", true);
        mapKeyword(off, "macro_keyword_id", macroOn);

        assertThat(targets()).noneMatch(t -> t.startsWith("kw off"));
    }

    @Test
    void tableRulesRejectBadRows() {
        long id = keyword("kw rule", true);
        long macroId = macro("MACRO RULE", true);
        mapKeyword(id, "etf_id", etfId("NEWE02"));

        // 정확히 하나만 가리킬 수 있다: 둘이거나 없으면 거부한다.
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO news_keyword_map (keyword_id, etf_id, stock_id) VALUES (:k, :e, :s)")
                .param("k", id).param("e", etfId("NEWE01")).param("s", stockId("NEWSA")).update())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO news_keyword_map (keyword_id) VALUES (:k)")
                .param("k", id).update()).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO macro_keyword_map (macro_keyword_id) VALUES (:m)")
                .param("m", macroId).update()).isInstanceOf(RuntimeException.class);
        // 같은 검색어를 같은 대상에 두 번 연결하거나 같은 검색어 글자를 두 번 넣을 수 없다.
        assertThatThrownBy(() -> mapKeyword(id, "etf_id", etfId("NEWE02"))).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> keyword("kw rule", true)).isInstanceOf(RuntimeException.class);
    }
}

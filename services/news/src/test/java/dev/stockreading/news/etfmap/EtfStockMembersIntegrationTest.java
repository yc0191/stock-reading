package dev.stockreading.news.etfmap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * ETF 구성 종목을 뉴스 계정으로 market.etf_stock_map에서 읽는 것을 실제 Postgres로 확인한다.
 * 가짜 ETF·종목(NEWM 접두어)을 시세처리기 계정(market 표의 주인)으로 넣고, 뉴스 계정은 읽기만 하는지도 본다.
 * 같은 DB의 다른 테스트와 겹치지 않게 NEWM 접두어 행만 지운다.
 */
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class EtfStockMembersIntegrationTest {

    private final JdbcClient news = JdbcClient.create(new DriverManagerDataSource(System.getenv("NEWS_IT_DB_URL"),
            System.getenv("NEWS_IT_DB_USER"), System.getenv("NEWS_IT_DB_PASSWORD")));
    private final JdbcClient market = JdbcClient.create(new DriverManagerDataSource(System.getenv("NEWS_IT_DB_URL"),
            System.getenv("MARKET_IT_DB_USER"), System.getenv("MARKET_IT_DB_PASSWORD")));

    @BeforeEach
    void seed() {
        String database = market.sql("SELECT current_database()").query(String.class).single();
        if (!database.startsWith("stock_test_run_")) {
            throw new IllegalStateException("실제 DB 테스트는 일회용 데이터베이스(stock_test_run_...)에서만 돌 수 있습니다: " + database);
        }
        market.sql("DELETE FROM etf_stock_map WHERE etf_id IN (SELECT id FROM etf WHERE symbol LIKE 'NEWM%')").update();
        market.sql("DELETE FROM etf WHERE symbol LIKE 'NEWM%'").update();
        market.sql("DELETE FROM stock WHERE symbol LIKE 'NEWM%'").update();
        market.sql("INSERT INTO etf (market, symbol) VALUES ('KR', 'NEWM01'), ('KR', 'NEWM02'), ('US', 'NEWM03')").update();
        market.sql("INSERT INTO stock (market, symbol) VALUES ('KR', 'NEWM04'), ('US', 'NEWMA'), ('US', 'NEWM-B')").update();
        map("NEWM01", "NEWM04");
        map("NEWM01", "NEWMA");
        map("NEWM02", "NEWMA");
        map("NEWM03", "NEWMB");
    }

    private void map(String etf, String stock) {
        market.sql("INSERT INTO etf_stock_map (etf_id, stock_id) SELECT e.id, s.id FROM etf e, stock s "
                + "WHERE e.symbol = :e AND s.symbol = :s").param("e", etf).param("s", stock).update();
    }

    @Test
    void membersOfAKoreanEtfAreItsDomesticCodesAndUsTickersFromTheEtfStockMap() {
        EtfStockMembers members = new EtfStockMembers(news);

        assertThat(members.membersOf("NEWM01")).containsExactlyInAnyOrder("NEWM04", "NEWMA");
        assertThat(members.membersOf("NEWM02")).containsExactly("NEWMA");
    }

    @Test
    void unknownEtfsAndNonKoreanEtfsHaveNoMembers() {
        EtfStockMembers members = new EtfStockMembers(news);

        assertThat(members.membersOf("NEWMZZ")).isEmpty();
        assertThat(members.membersOf("NEWM03")).isEmpty();
    }

    @Test
    void theNewsAccountOnlyReadsTheMapAndNeverWritesIt() {
        assertThat(news.sql("SELECT count(*) FROM market.etf_stock_map").query(Long.class).single()).isPositive();
        assertThatThrownBy(() -> news.sql("DELETE FROM market.etf_stock_map").update())
                .rootCause().hasMessageContaining("permission denied");
        assertThatThrownBy(() -> news.sql("INSERT INTO market.stock (market, symbol) VALUES ('US', 'NEWMX')").update())
                .rootCause().hasMessageContaining("permission denied");
    }
}

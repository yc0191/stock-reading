package dev.stockreading.market.constituent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import dev.stockreading.market.DisposableDatabase;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * ETF·주식 표와 연결 표, 개인 포트폴리오로 계산하는 보유·관심 구분, 목록 조회를 실제 Postgres로 확인한다.
 * 환경변수가 모두 있을 때만 실행하며, 시세처리기 계정으로 market 표를 읽고 쓰기 계정(WRITER_IT_DB_*)으로 personal 표를 채운다.
 * personal 스키마가 먼저 만들어져 있어야 한다(전체 실행 스크립트가 준비한다). 이 테스트는 시작할 때 두 스키마의 관련 표를 비운다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
class HoldingsIntegrationTest {

    private static JdbcClient jdbc;
    private static JdbcClient writer;
    private static HoldingsRepository repository;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");
        DisposableDatabase.require(JdbcClient.create(new DriverManagerDataSource(url, user, password)));
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();
        DriverManagerDataSource dataSource = new DriverManagerDataSource(url, user, password);
        jdbc = JdbcClient.create(dataSource);
        writer = JdbcClient.create(new DriverManagerDataSource(url, System.getenv("WRITER_IT_DB_USER"),
                System.getenv("WRITER_IT_DB_PASSWORD")));
        repository = new HoldingsRepository(jdbc);
    }

    @BeforeEach
    void startEmpty() {
        // 포트폴리오가 종목 행을 가리키므로 personal 쪽을 먼저 비운다.
        writer.sql("DELETE FROM personal.member_portfolio").update();
        writer.sql("DELETE FROM personal.member").update();
        jdbc.sql("DELETE FROM etf_stock_map").update();
        jdbc.sql("DELETE FROM etf").update();
        jdbc.sql("DELETE FROM stock").update();
        etf("TEST01", "TEST ETF ONE", "TEST INDEX");
        etf("TEST02", null, null);
        etf("TEST03", null, null);
        stock("US", "FAKEA", "FAKE CORP A");
        stock("US", "FAKE-B", null);
        stock("KR", "TEST09", null);
        map("TEST01", "FAKEA", "12.5", 1);
        map("TEST01", "FAKE-B", "8.25", 2);
        map("TEST01", "TEST09", null, 3);
        map("TEST02", "FAKEA", "30", 1);
    }

    private static void etf(String symbol, String name, String tracks) {
        jdbc.sql("INSERT INTO etf (market, symbol, name, tracks) VALUES ('KR', :symbol, :name, :tracks)")
                .param("symbol", symbol).param("name", name).param("tracks", tracks).update();
    }

    private static void stock(String market, String symbol, String name) {
        jdbc.sql("INSERT INTO stock (market, symbol, name) VALUES (:market, :symbol, :name)")
                .param("market", market).param("symbol", symbol).param("name", name).update();
    }

    private static void map(String etf, String stock, String weight, int rank) {
        jdbc.sql("""
                        INSERT INTO etf_stock_map (etf_id, stock_id, weight, rank)
                        VALUES ((SELECT id FROM etf WHERE symbol = :etf), (SELECT id FROM stock WHERE symbol = :stock),
                                :weight, :rank)
                        """)
                .param("etf", etf).param("stock", stock)
                .param("weight", weight == null ? null : new BigDecimal(weight)).param("rank", rank).update();
    }

    private static long member(String name) {
        return writer.sql("INSERT INTO personal.member (name) VALUES (:name) RETURNING id").param("name", name)
                .query(Long.class).single();
    }

    private static void portfolioEtf(long memberId, String etf, String relation) {
        writer.sql("""
                        INSERT INTO personal.member_portfolio (member_id, etf_id, relation)
                        VALUES (:member, (SELECT id FROM market.etf WHERE symbol = :etf), :relation)
                        """)
                .param("member", memberId).param("etf", etf).param("relation", relation).update();
    }

    private static void portfolioStock(long memberId, String stock, String relation) {
        writer.sql("""
                        INSERT INTO personal.member_portfolio (member_id, stock_id, relation)
                        VALUES (:member, (SELECT id FROM market.stock WHERE symbol = :stock), :relation)
                        """)
                .param("member", memberId).param("stock", stock).param("relation", relation).update();
    }

    @Test
    void sharedStockIsOneStockRowAndEachEtfGetsItsOwnMapRow() {
        assertThat(count("etf")).isEqualTo(3);
        assertThat(count("stock")).isEqualTo(3);
        assertThat(count("etf_stock_map")).isEqualTo(4);
        assertThat(jdbc.sql("SELECT count(*) FROM stock WHERE symbol = 'FAKEA'").query(Long.class).single())
                .isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_map m JOIN stock s ON s.id = m.stock_id "
                + "WHERE s.symbol = 'FAKEA'").query(Long.class).single()).isEqualTo(2);
    }

    @Test
    void stockNameIsReadFromTheKrStockTableAndMissingOrBlankNamesAreEmpty() {
        stock("KR", "TEST07", "TEST STOCK SEVEN");
        stock("KR", "TEST08", "  ");

        assertThat(repository.stockName("TEST07")).contains("TEST STOCK SEVEN");
        assertThat(repository.stockName("TEST08")).isEmpty();
        // 이름 칸이 비어 있는 행, 없는 코드, 미국 종목은 국내 종목 이름으로 찾지 않는다.
        assertThat(repository.stockName("TEST09")).isEmpty();
        assertThat(repository.stockName("TEST99")).isEmpty();
        assertThat(repository.stockName("FAKEA")).isEmpty();
    }

    @Test
    void etfInfoAndTickersAreReadBackInRankOrder() {
        assertThat(repository.topTickers("TEST01")).containsExactly("FAKEA", "FAKE-B");
        assertThat(repository.topTickers("TEST99")).isEmpty();
        EtfInfo info = repository.etfInfo("TEST01").orElseThrow();
        assertThat(info.name()).isEqualTo("TEST ETF ONE");
        assertThat(info.tracks()).isEqualTo("TEST INDEX");
        assertThat(info.sector()).isNull();
        assertThat(info.members()).extracting(EtfInfo.Member::symbol).containsExactly("FAKEA", "FAKE-B", "TEST09");
        assertThat(info.members().get(0).weight()).isEqualByComparingTo(new BigDecimal("12.5"));
        assertThat(info.members().get(0).name()).isEqualTo("FAKE CORP A");
        assertThat(info.members().get(2).weight()).isNull();
        assertThat(repository.etfInfo("TEST99")).isEmpty();
        // 구성 종목은 ETF가 아니므로 ETF 정보가 없다.
        assertThat(repository.etfInfo("TEST09")).isEmpty();
    }

    @Test
    void aPortfolioOfADisabledMemberDoesNotCount() {
        long off = member("MEMBER OFF");
        writer.sql("UPDATE personal.member SET enabled = FALSE WHERE id = :id").param("id", off).update();
        portfolioEtf(off, "TEST02", "HELD");
        portfolioEtf(off, "TEST03", "WATCH");
        portfolioEtf(member("MEMBER ON"), "TEST01", "WATCH");

        assertThat(repository.trackedEtfs()).containsExactly("TEST01");
    }

    @Test
    void trackedEtfsAreKrEtfsInAPortfolioInCodeOrderAndLeaveOutOthers() {
        long first = member("MEMBER ONE");
        portfolioEtf(first, "TEST03", "WATCH");
        portfolioEtf(first, "TEST01", "HELD");
        portfolioStock(first, "FAKEA", "HELD");

        assertThat(repository.trackedEtfs()).containsExactly("TEST01", "TEST03");
    }

    // 보유·관심 구분은 읽지 않는다. 같은 ETF를 두 멤버가 서로 다르게(보유/관심) 담아도 한 번만 나온다.
    @Test
    void anEtfTwoMembersHoldDifferentlyIsListedOnce() {
        portfolioEtf(member("MEMBER ONE"), "TEST01", "HELD");
        portfolioEtf(member("MEMBER TWO"), "TEST01", "WATCH");

        assertThat(repository.trackedEtfs()).containsExactly("TEST01");
    }

    @Test
    void trackedEtfsAreEmptyWhenNobodyHoldsAnEtf() {
        assertThat(repository.trackedEtfs()).isEmpty();
    }

    @Test
    void aStockInAPortfolioCannotBeDeleted() {
        portfolioStock(member("MEMBER ONE"), "FAKEA", "HELD");

        assertThatThrownBy(() -> jdbc.sql("DELETE FROM stock WHERE symbol = 'FAKEA'").update())
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void tableRulesRejectBadRows() {
        assertThatThrownBy(() -> stock("US", "FAKEA", "DUPLICATE")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> etf("TEST01", "DUPLICATE", null)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("UPDATE etf_stock_map SET weight = 100.5").update())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("UPDATE etf_stock_map SET rank = 0").update())
                .isInstanceOf(RuntimeException.class);
    }

    private static long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }
}

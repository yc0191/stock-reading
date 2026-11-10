package dev.stockreading.market.holdings;

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
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

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
        repository = new HoldingsRepository(jdbc,
                new TransactionTemplate(new DataSourceTransactionManager(dataSource)));
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
    void relationIsHeldIfAnyMemberHoldsItElseWatchIfAnyMemberWatchesItElseNone() {
        long first = member("MEMBER ONE");
        long second = member("MEMBER TWO");
        portfolioEtf(first, "TEST01", "WATCH");
        portfolioEtf(second, "TEST01", "HELD");
        portfolioEtf(first, "TEST02", "WATCH");
        portfolioStock(second, "FAKEA", "HELD");

        HoldingsSnapshot snapshot = repository.readSnapshot();

        assertThat(snapshot.instruments()).hasSize(6);
        assertThat(relationOf(snapshot, "TEST01")).isEqualTo("HELD");
        assertThat(relationOf(snapshot, "TEST02")).isEqualTo("WATCH");
        assertThat(relationOf(snapshot, "TEST03")).isEqualTo("NONE");
        assertThat(relationOf(snapshot, "FAKEA")).isEqualTo("HELD");
        assertThat(relationOf(snapshot, "FAKE-B")).isEqualTo("NONE");
        assertThat(snapshot.instruments()).filteredOn(i -> i.symbol().startsWith("TEST0") && !i.symbol().equals("TEST09"))
                .extracting(i -> i.kind()).containsOnly("ETF");
        assertThat(snapshot.instruments()).filteredOn(i -> i.symbol().equals("FAKEA"))
                .extracting(i -> i.kind()).containsExactly("STOCK");
    }

    @Test
    void aPortfolioOfADisabledMemberDoesNotCount() {
        long off = member("MEMBER OFF");
        writer.sql("UPDATE personal.member SET enabled = FALSE WHERE id = :id").param("id", off).update();
        portfolioEtf(off, "TEST02", "HELD");
        portfolioEtf(off, "TEST03", "WATCH");
        portfolioEtf(member("MEMBER ON"), "TEST01", "WATCH");

        HoldingsSnapshot snapshot = repository.readSnapshot();

        assertThat(relationOf(snapshot, "TEST01")).isEqualTo("WATCH");
        assertThat(relationOf(snapshot, "TEST02")).isEqualTo("NONE");
        assertThat(relationOf(snapshot, "TEST03")).isEqualTo("NONE");
        assertThat(repository.trackedEtfs()).containsExactly(new HoldingsPort.TrackedEtf("TEST01", "WATCH"));
    }

    private static String relationOf(HoldingsSnapshot snapshot, String symbol) {
        return snapshot.instruments().stream().filter(i -> i.symbol().equals(symbol)).findFirst().orElseThrow()
                .relation();
    }

    @Test
    void trackedEtfsAreKrEtfsInAPortfolioInCodeOrderWithTheirRelationAndLeaveOutOthers() {
        long first = member("MEMBER ONE");
        portfolioEtf(first, "TEST03", "WATCH");
        portfolioEtf(first, "TEST01", "HELD");
        portfolioStock(first, "FAKEA", "HELD");

        assertThat(repository.trackedEtfs()).containsExactly(
                new HoldingsPort.TrackedEtf("TEST01", "HELD"), new HoldingsPort.TrackedEtf("TEST03", "WATCH"));
    }

    @Test
    void trackedEtfsAreEmptyWhenNobodyHoldsAnEtf() {
        assertThat(repository.trackedEtfs()).isEmpty();
    }

    @Test
    void readSnapshotReturnsTheWholeListWithNamesAndRanksAndTheHashChangesOnlyWhenTheTablesChange() {
        HoldingsSnapshot snapshot = repository.readSnapshot();

        assertThat(snapshot.instruments()).hasSize(6);
        assertThat(snapshot.memberships()).hasSize(4);
        assertThat(snapshot.instruments()).anySatisfy(i -> {
            assertThat(i.symbol()).isEqualTo("FAKEA");
            assertThat(i.name()).isEqualTo("FAKE CORP A");
            assertThat(i.relation()).isEqualTo("NONE");
        });
        assertThat(snapshot.instruments()).anySatisfy(i -> {
            assertThat(i.symbol()).isEqualTo("FAKE-B");
            assertThat(i.name()).isNull();
        });
        assertThat(snapshot.memberships()).anySatisfy(m -> {
            assertThat(m.etfSymbol()).isEqualTo("TEST01");
            assertThat(m.memberSymbol()).isEqualTo("FAKE-B");
            assertThat(m.rank()).isEqualTo(2);
        });
        String before = snapshot.contentHash();
        // updated_at만 바뀌어도 해시는 같다.
        jdbc.sql("UPDATE etf SET updated_at = now()").update();
        assertThat(repository.readSnapshot().contentHash()).isEqualTo(before);
        // 개인 포트폴리오가 바뀌면 relation이 달라져 해시가 달라진다.
        portfolioEtf(member("MEMBER ONE"), "TEST01", "WATCH");
        String afterPortfolio = repository.readSnapshot().contentHash();
        assertThat(afterPortfolio).isNotEqualTo(before);
        // 구성 관계를 직접 고쳐도 달라진다.
        jdbc.sql("UPDATE etf_stock_map SET rank = 9 WHERE rank = 2").update();
        assertThat(repository.readSnapshot().contentHash()).isNotEqualTo(afterPortfolio);
    }

    @Test
    void readSnapshotKeepsANullRankWhenTheTableRowHasNone() {
        stock("US", "FAKENR", null);
        jdbc.sql("INSERT INTO etf_stock_map (etf_id, stock_id) "
                + "SELECT e.id, s.id FROM etf e, stock s WHERE e.symbol = 'TEST01' AND s.symbol = 'FAKENR'").update();

        assertThat(repository.readSnapshot().memberships()).anySatisfy(m -> {
            assertThat(m.memberSymbol()).isEqualTo("FAKENR");
            assertThat(m.rank()).isNull();
        });
    }

    @Test
    void emptyTablesGiveAnEmptyList() {
        jdbc.sql("DELETE FROM etf_stock_map").update();
        jdbc.sql("DELETE FROM etf").update();
        jdbc.sql("DELETE FROM stock").update();

        HoldingsSnapshot snapshot = repository.readSnapshot();

        assertThat(snapshot.instruments()).isEmpty();
        assertThat(snapshot.memberships()).isEmpty();
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

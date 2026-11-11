package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

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

import tools.jackson.databind.json.JsonMapper;

/**
 * 구성 종목 이력과 교체 일정 테이블의 마이그레이션, 이력 쓰기와 ETF 구성 연결 표 맞추기, 판 범위 안 변경 읽기를 실제 Postgres로 확인한다.
 * 세 환경변수가 모두 있을 때만 실행하며, 대상은 비어 있는 새 DB(public 스키마)라고 가정한다.
 * 가짜 식별자는 MEMBER 접두어로 정해 다른 통합 테스트(TEST, FAKE 접두어)와 겹치지 않게 한다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
class MemberIntegrationTest {

    private static JdbcClient jdbc;
    private static JdbcClient writer;
    private static MemberRepository repository;
    private long etfId;

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
        repository = new MemberRepository(jdbc, new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
                JsonMapper.builder().build());
    }

    @BeforeEach
    void startWithOneEtf() {
        jdbc.sql("DELETE FROM index_rebalance_schedule").update();
        // 다른 테스트 클래스가 남긴 개인 포트폴리오가 종목 행을 가리키면 지울 수 없으므로 먼저 비운다.
        writer.sql("DELETE FROM personal.member_portfolio").update();
        writer.sql("DELETE FROM personal.member").update();
        jdbc.sql("DELETE FROM etf_stock_map").update();
        jdbc.sql("DELETE FROM etf").update();
        jdbc.sql("DELETE FROM stock").update();
        etfId = jdbc.sql("""
                        INSERT INTO etf (market, symbol, name, tracks, constituent_source_id)
                        VALUES ('KR', 'MEMBE1', 'MEMBER ETF', 'MEMBER INDEX', 'SRC1') RETURNING id
                        """)
                .query(Long.class).single();
        // 직접 넣은 주식은 구성에서 빠져도 지워지지 않아야 한다.
        jdbc.sql("INSERT INTO stock (market, symbol) VALUES ('US', 'MEMBX')").update();
    }

    private static List<Member> members(String... symbols) {
        java.util.ArrayList<Member> list = new java.util.ArrayList<>();
        for (String symbol : symbols) {
            list.add(new Member("US", symbol, "NAME " + symbol, "5.5", list.size() + 1));
        }
        return list;
    }

    @Test
    void targetsAreTheKoreanEtfsWithASourceIdWithTheirIndex() {
        jdbc.sql("INSERT INTO etf (market, symbol, constituent_source_id) VALUES ('KR', 'MEMBE2', NULL)").update();
        jdbc.sql("INSERT INTO etf (market, symbol, constituent_source_id) VALUES ('KR', 'MEMBE3', '  ')").update();
        jdbc.sql("INSERT INTO etf (market, symbol, constituent_source_id) VALUES ('US', 'MEMBE4', 'SRC4')").update();

        assertThat(repository.targets())
                .containsExactly(new MemberRepository.Target(etfId, "MEMBE1", "MEMBER INDEX", "SRC1"));
    }

    @Test
    void recordingACompositionWritesHistoryAndMakesTheEtfStockMapTheSame() {
        repository.record(etfId, members("MEMBA", "MEMBB"), LocalDate.parse("2026-11-02"));

        MemberRepository.Latest latest = repository.latest(etfId).orElseThrow();
        assertThat(latest.members()).isEqualTo(members("MEMBA", "MEMBB"));
        assertThat(latest.lastSeenOn()).isEqualTo(LocalDate.parse("2026-11-02"));
        List<String> rows = jdbc.sql("""
                        SELECT s.symbol || ':' || m.rank || ':' || m.weight FROM etf_stock_map m
                        JOIN stock s ON s.id = m.stock_id WHERE m.etf_id = :etf ORDER BY m.rank
                        """)
                .param("etf", etfId).query(String.class).list();
        assertThat(rows).containsExactly("MEMBA:1:5.5000", "MEMBB:2:5.5000");
        assertThat(jdbc.sql("SELECT name FROM stock WHERE symbol = 'MEMBA'").query(String.class).single())
                .isEqualTo("NAME MEMBA");
    }

    @Test
    void aChangedCompositionReplacesTheMapRowsAndNeverDeletesStocks() {
        repository.record(etfId, members("MEMBA", "MEMBX"), LocalDate.parse("2026-11-02"));
        repository.record(etfId, members("MEMBB"), LocalDate.parse("2026-11-09"));

        // 구성에서 빠진 MEMBA도 stock 행은 남는다(개인 포트폴리오가 가리킬 수 있다).
        assertThat(jdbc.sql("SELECT symbol FROM stock ORDER BY symbol")
                .query(String.class).list()).containsExactly("MEMBA", "MEMBB", "MEMBX");
        assertThat(jdbc.sql("SELECT symbol FROM stock s JOIN etf_stock_map m ON m.stock_id = s.id WHERE m.etf_id = :etf")
                .param("etf", etfId).query(String.class).list()).containsExactly("MEMBB");
        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_map WHERE etf_id = :etf").param("etf", etfId)
                .query(Long.class).single()).isEqualTo(1L);
        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_history WHERE etf_id = :etf").param("etf", etfId)
                .query(Long.class).single()).isEqualTo(2L);
    }

    @Test
    void touchMovesOnlyTheLastSeenDayForwardAndNeverBackward() {
        repository.record(etfId, members("MEMBA"), LocalDate.parse("2026-11-02"));
        long id = repository.latest(etfId).orElseThrow().id();

        repository.touch(id, etfId, members("MEMBA"), LocalDate.parse("2026-11-09"));
        repository.touch(id, etfId, members("MEMBA"), LocalDate.parse("2026-11-03"));

        assertThat(repository.latest(etfId).orElseThrow().lastSeenOn()).isEqualTo(LocalDate.parse("2026-11-09"));
    }

    @Test
    void touchRefreshesTheMapWeightWithoutAddingHistory() {
        repository.record(etfId, members("MEMBA"), LocalDate.parse("2026-11-02"));
        long id = repository.latest(etfId).orElseThrow().id();

        repository.touch(id, etfId, List.of(new Member("US", "MEMBA", "NAME MEMBA", "7.25", 1)),
                LocalDate.parse("2026-11-09"));

        assertThat(jdbc.sql("SELECT weight FROM etf_stock_map WHERE etf_id = :etf").param("etf", etfId)
                .query(java.math.BigDecimal.class).single()).isEqualByComparingTo("7.25");
        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_history WHERE etf_id = :etf").param("etf", etfId)
                .query(Long.class).single()).isEqualTo(1L);
    }

    @Test
    void aSourceNameNeverOverwritesAnExistingNameButFillsAnEmptyOne() {
        jdbc.sql("UPDATE stock SET name = 'MANUAL NAME' WHERE symbol = 'MEMBX'").update();
        jdbc.sql("INSERT INTO stock (market, symbol) VALUES ('US', 'MEMBN')").update();

        repository.record(etfId, members("MEMBX", "MEMBN"), LocalDate.parse("2026-11-02"));

        assertThat(jdbc.sql("SELECT name FROM stock WHERE symbol = 'MEMBX'").query(String.class).single())
                .isEqualTo("MANUAL NAME");
        assertThat(jdbc.sql("SELECT name FROM stock WHERE symbol = 'MEMBN'").query(String.class).single())
                .isEqualTo("NAME MEMBN");
    }

    @Test
    void recordingAnotherEtfWithTheSameStockKeepsBothEtfsMapRows() {
        long other = jdbc.sql("INSERT INTO etf (market, symbol) VALUES ('KR', 'MEMBE5') RETURNING id")
                .query(Long.class).single();

        repository.record(etfId, members("MEMBA"), LocalDate.parse("2026-11-02"));
        repository.record(other, members("MEMBA"), LocalDate.parse("2026-11-02"));
        repository.record(etfId, members("MEMBB"), LocalDate.parse("2026-11-09"));

        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_map WHERE etf_id = :etf").param("etf", other)
                .query(Long.class).single()).isEqualTo(1L);
        assertThat(jdbc.sql("SELECT count(*) FROM stock WHERE symbol = 'MEMBA'").query(Long.class).single())
                .isEqualTo(1L);
    }

    @Test
    void rawResponsesAreStoredVerbatimIncludingFailedOnes() {
        repository.saveRaw(etfId, LocalDate.parse("2026-11-06"), 200, " {\"pdf\": 1}\n");
        repository.saveRaw(etfId, LocalDate.parse("2026-11-06"), 503, "down");

        assertThat(jdbc.sql("SELECT http_status || ':' || body FROM etf_stock_raw WHERE etf_id = :etf ORDER BY id")
                .param("etf", etfId).query(String.class).list()).containsExactly("200: {\"pdf\": 1}\n", "503:down");
    }

    @Test
    void changesAppliedInsideTheRangeAreReadWithTheDaysTheyWereSeen() {
        repository.record(etfId, members("MEMBA", "MEMBB"), LocalDate.parse("2026-11-02"));
        long first = repository.latest(etfId).orElseThrow().id();
        repository.touch(first, etfId, members("MEMBA", "MEMBB"), LocalDate.parse("2026-11-08"));
        repository.record(etfId, members("MEMBA", "MEMBC"), LocalDate.parse("2026-11-09"));

        OffsetDateTime now = OffsetDateTime.now();
        MemberChanges changes = repository.changes("MEMBE1", now.minusHours(1), now.plusHours(1)).orElseThrow();

        assertThat(changes.since()).isEqualTo(LocalDate.parse("2026-11-08"));
        assertThat(changes.asOf()).isEqualTo(LocalDate.parse("2026-11-09"));
        assertThat(changes.removed()).extracting(Member::symbol).containsExactly("MEMBB");
        assertThat(changes.added()).extracting(Member::symbol).containsExactly("MEMBC");
        // 범위가 변경 반영 이후에 시작하면 그 변경은 싣지 않는다. 범위 끝이 반영 이전이면 아직 없는 변경이다.
        assertThat(repository.changes("MEMBE1", now.plusHours(1), now.plusHours(2))).isEmpty();
        assertThat(repository.changes("MEMBE1", now.minusDays(2), now.minusHours(1))).isEmpty();
    }

    @Test
    void rebalanceMonthIsTheCalendarMonthOfAnEffectiveDateForThatIndexOnly() {
        jdbc.sql("INSERT INTO index_rebalance_schedule (tracks, effective_date, note) VALUES ('MEMBER INDEX', DATE '2026-12-18', 'TEST')")
                .update();

        assertThat(repository.inRebalanceMonth("MEMBER INDEX", LocalDate.parse("2026-12-01"))).isTrue();
        assertThat(repository.inRebalanceMonth("MEMBER INDEX", LocalDate.parse("2026-12-31"))).isTrue();
        assertThat(repository.inRebalanceMonth("MEMBER INDEX", LocalDate.parse("2026-11-30"))).isFalse();
        assertThat(repository.inRebalanceMonth("MEMBER INDEX", LocalDate.parse("2027-01-01"))).isFalse();
        assertThat(repository.inRebalanceMonth("OTHER INDEX", LocalDate.parse("2026-12-18"))).isFalse();
        assertThat(repository.inRebalanceMonth(null, LocalDate.parse("2026-12-18"))).isFalse();
    }
}

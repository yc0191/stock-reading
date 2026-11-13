package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

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
 * 구성 종목 이력·원문 테이블의 마이그레이션, 받은 결과의 원문 저장·비교·반영(같은 이벤트는 한 번), 이력 쓰기와 ETF 구성 연결 표 맞추기,
 * 판 범위 안 변경 읽기를 실제 Postgres로 확인한다.
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
    private static TransactionTemplate transaction;
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
        transaction = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        repository = new MemberRepository(jdbc, transaction, JsonMapper.builder().build());
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
    void theEtfIdIsFoundByItsKoreanSymbolOnly() {
        jdbc.sql("INSERT INTO etf (market, symbol) VALUES ('US', 'MEMBE4')").update();

        assertThat(repository.etfId("MEMBE1")).contains(etfId);
        assertThat(repository.etfId("MEMBE4")).isEmpty();
        assertThat(repository.etfId("MEMBE9")).isEmpty();
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
    void rawResponsesAreStoredVerbatimIncludingFailedOnesAndOnlyOncePerEvent() {
        UUID first = UUID.randomUUID();
        OffsetDateTime fetchedAt = OffsetDateTime.parse("2026-11-09T08:00:01+09:00");
        assertThat(repository.saveRaw(first, etfId, LocalDate.parse("2026-11-06"), 200, " {\"pdf\": 1}\n", fetchedAt)).isTrue();
        assertThat(repository.saveRaw(UUID.randomUUID(), etfId, LocalDate.parse("2026-11-06"), 503, "down", fetchedAt)).isTrue();
        assertThat(repository.saveRaw(first, etfId, LocalDate.parse("2026-11-06"), 200, "again", fetchedAt)).isFalse();
        // 받은 시각은 저장한 시각이 아니라 수집기가 응답을 받은 시각이다.
        assertThat(jdbc.sql("SELECT DISTINCT fetched_at FROM etf_stock_raw WHERE etf_id = :etf").param("etf", etfId)
                .query(OffsetDateTime.class).list()).singleElement().satisfies(t -> assertThat(t).isAtSameInstantAs(fetchedAt));

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
    void aFetchedResultIsSavedComparedAndAppliedOnceAndAFailureKeepsTheComposition() {
        MemberApplyService service = new MemberApplyService(repository, transaction);
        UUID event = UUID.randomUUID();

        assertThat(service.apply(event, payload(members("MEMBA", "MEMBB"), "2026-11-02", null)))
                .isEqualTo(MemberApplyService.Result.FIRST);
        // 같은 이벤트가 다시 와도 이력·원문이 늘지 않는다.
        assertThat(service.apply(event, payload(members("MEMBA", "MEMBB"), "2026-11-02", null)))
                .isEqualTo(MemberApplyService.Result.DUPLICATE);
        assertThat(service.apply(UUID.randomUUID(), payload(members("MEMBA", "MEMBB"), "2026-11-09", null)))
                .isEqualTo(MemberApplyService.Result.SAME);
        assertThat(service.apply(UUID.randomUUID(), payload(null, "2026-11-10", "unexpected status 503")))
                .isEqualTo(MemberApplyService.Result.FAILED_KEPT);
        assertThat(service.apply(UUID.randomUUID(), payload(members("MEMBC"), "2026-11-16", null)))
                .isEqualTo(MemberApplyService.Result.CHANGED);

        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_raw WHERE etf_id = :etf").param("etf", etfId)
                .query(Long.class).single()).isEqualTo(4L);
        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_history WHERE etf_id = :etf").param("etf", etfId)
                .query(Long.class).single()).isEqualTo(2L);
        assertThat(jdbc.sql("SELECT s.symbol FROM etf_stock_map m JOIN stock s ON s.id = m.stock_id WHERE m.etf_id = :etf")
                .param("etf", etfId).query(String.class).list()).containsExactly("MEMBC");
    }

    @Test
    void aFailureWhileApplyingLeavesNoRawSoTheRedeliveryIsAppliedAgain() {
        MemberApplyService service = new MemberApplyService(repository, transaction);
        UUID event = UUID.randomUUID();
        // 이름 칸 길이(100자)를 넘는 값은 반영(stock 넣기)에서 실패한다. 원문 저장도 함께 되돌려져야 한다.
        List<Member> tooLong = List.of(new Member("US", "MEMBA", "N".repeat(150), "5.5", 1));

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> service.apply(event, payload(tooLong, "2026-11-02", null)));

        assertThat(jdbc.sql("SELECT count(*) FROM etf_stock_raw WHERE event_id = :e").param("e", event)
                .query(Long.class).single()).isZero();
        assertThat(service.apply(event, payload(members("MEMBA"), "2026-11-02", null)))
                .isEqualTo(MemberApplyService.Result.FIRST);
    }

    private static MembersFetchedEnvelope.Payload payload(List<Member> members, String fetchedOn, String failure) {
        return new MembersFetchedEnvelope.Payload("MEMBE1", "2026-11-06", fetchedOn, fetchedOn + "T08:00:01+09:00",
                failure == null ? 200 : 503, "RAW " + fetchedOn, members, failure);
    }
}

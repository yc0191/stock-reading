package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.databind.json.JsonMapper;

/**
 * 종목·ETF 구성 테이블의 마이그레이션, 파일과 똑같이 맞추기, 조회를 실제 Postgres로 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 비어 있는 새 DB(public 스키마)라고 가정한다. 이 테이블만 쓰므로 다른 테스트 클래스와 데이터가 겹치지 않는다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class HoldingsIntegrationTest {

    private static final String FIRST = """
            [
              {"kind": "ETF", "market": "KR", "symbol": "TEST01", "name": "TEST ETF ONE", "tracks": "TEST INDEX",
               "sector": "TEST SECTOR", "relation": "HELD",
               "members": [
                 {"market": "US", "symbol": "FAKEA", "name": "FAKE CORP A", "weight": 12.5, "rank": 1},
                 {"market": "US", "symbol": "FAKE.B", "weight": 8.25, "rank": 2},
                 {"market": "KR", "symbol": "TEST09", "rank": 3}]},
              {"kind": "ETF", "market": "KR", "symbol": "TEST02", "relation": "WATCH",
               "members": [{"market": "US", "symbol": "FAKEA", "weight": 30}]}
            ]
            """;

    private static JdbcClient jdbc;
    private static HoldingsRepository repository;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();
        DriverManagerDataSource dataSource = new DriverManagerDataSource(url, user, password);
        jdbc = JdbcClient.create(dataSource);
        repository = new HoldingsRepository(jdbc,
                new TransactionTemplate(new DataSourceTransactionManager(dataSource)));
    }

    @BeforeEach
    void startEmpty() {
        repository.replaceAll(HoldingsPlan.empty());
    }

    private static HoldingsPlan plan(String json) {
        return HoldingsFileParser.parse(json, JsonMapper.builder().build());
    }

    @Test
    void sharedStockIsOneInstrumentRowAndEachEtfGetsItsOwnMemberRow() {
        repository.replaceAll(plan(FIRST));

        assertThat(count("instrument")).isEqualTo(5);
        assertThat(count("etf_member")).isEqualTo(4);
        assertThat(jdbc.sql("SELECT count(*) FROM instrument WHERE symbol = 'FAKEA'").query(Long.class).single())
                .isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM etf_member em JOIN instrument m ON m.id = em.member_id "
                + "WHERE m.symbol = 'FAKEA'").query(Long.class).single()).isEqualTo(2);
    }

    @Test
    void etfInfoAndTickersAreReadBackInRankOrder() {
        repository.replaceAll(plan(FIRST));

        assertThat(repository.topTickers("TEST01")).containsExactly("FAKEA", "FAKE-B");
        assertThat(repository.topTickers("TEST99")).isEmpty();
        EtfInfo info = repository.etfInfo("TEST01").orElseThrow();
        assertThat(info.name()).isEqualTo("TEST ETF ONE");
        assertThat(info.tracks()).isEqualTo("TEST INDEX");
        assertThat(info.sector()).isEqualTo("TEST SECTOR");
        assertThat(info.members()).extracting(EtfInfo.Member::symbol).containsExactly("FAKEA", "FAKE-B", "TEST09");
        assertThat(info.members().get(0).weight()).isEqualByComparingTo(new BigDecimal("12.5"));
        assertThat(info.members().get(0).name()).isEqualTo("FAKE CORP A");
        assertThat(info.members().get(2).weight()).isNull();
        assertThat(repository.etfInfo("TEST99")).isEmpty();
        // 구성 종목은 ETF가 아니므로 ETF 정보가 없다.
        assertThat(repository.etfInfo("TEST09")).isEmpty();
    }

    @Test
    void trackedEtfsAreKrEtfsHeldOrWatchedInCodeOrderWithTheirRelationAndLeaveOutNoneAndMembers() {
        repository.replaceAll(plan("""
                [{"kind": "ETF", "market": "KR", "symbol": "TEST03", "relation": "WATCH"},
                 {"kind": "ETF", "market": "KR", "symbol": "TEST01", "relation": "HELD",
                  "members": [{"market": "US", "symbol": "FAKEA", "rank": 1}]},
                 {"kind": "ETF", "market": "KR", "symbol": "TEST02", "relation": "NONE"},
                 {"kind": "ETF", "market": "KR", "symbol": "TEST04"}]
                """));

        assertThat(repository.trackedEtfs()).containsExactly(
                new HoldingsPort.TrackedEtf("TEST01", "HELD"), new HoldingsPort.TrackedEtf("TEST03", "WATCH"));
        assertThat(repository.trackedEtfs()).extracting(HoldingsPort.TrackedEtf::relation)
                .containsExactly("HELD", "WATCH");
    }

    @Test
    void trackedEtfsAreEmptyWhenTheTableIsEmpty() {
        assertThat(repository.trackedEtfs()).isEmpty();
    }

    @Test
    void syncingAgainWithADifferentFileMakesTheTablesIdenticalToIt() {
        repository.replaceAll(plan(FIRST));
        repository.replaceAll(plan("""
                [{"kind": "ETF", "market": "KR", "symbol": "TEST01", "name": "TEST ETF RENAMED",
                  "members": [{"market": "US", "symbol": "FAKEC", "weight": 50}]}]
                """));

        assertThat(count("instrument")).isEqualTo(2);
        assertThat(count("etf_member")).isEqualTo(1);
        assertThat(repository.topTickers("TEST01")).containsExactly("FAKEC");
        assertThat(repository.topTickers("TEST02")).isEmpty();
        assertThat(repository.etfInfo("TEST01").orElseThrow().name()).isEqualTo("TEST ETF RENAMED");
        assertThat(repository.etfInfo("TEST01").orElseThrow().tracks()).isNull();
    }

    @Test
    void syncingTheSameFileTwiceChangesNothing() {
        repository.replaceAll(plan(FIRST));
        repository.replaceAll(plan(FIRST));

        assertThat(count("instrument")).isEqualTo(5);
        assertThat(count("etf_member")).isEqualTo(4);
    }

    @Test
    void isEmptyReflectsWhetherTheInstrumentTableHasRows() {
        assertThat(repository.isEmpty()).isTrue();

        repository.replaceAll(plan(FIRST));

        assertThat(repository.isEmpty()).isFalse();
    }

    @Test
    void readSnapshotReturnsTheWholeListWithNamesAndRanksAndTheHashChangesOnlyWhenTheTablesChange() {
        repository.replaceAll(plan(FIRST));

        HoldingsSnapshot snapshot = repository.readSnapshot();

        assertThat(snapshot.instruments()).hasSize(5);
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
        assertThat(snapshot.memberships()).anySatisfy(m -> {
            assertThat(m.etfSymbol()).isEqualTo("TEST02");
            // 파일에 rank가 없으면 파서가 적힌 순서(1부터)로 채운다.
            assertThat(m.rank()).isEqualTo(1);
        });
        String before = snapshot.contentHash();
        // 같은 내용으로 다시 채워도(updated_at만 바뀜) 해시는 같다.
        repository.replaceAll(plan(FIRST));
        assertThat(repository.readSnapshot().contentHash()).isEqualTo(before);
        // 테이블을 직접 고치면 해시가 달라진다.
        jdbc.sql("UPDATE instrument SET relation = 'WATCH' WHERE symbol = 'TEST01'").update();
        assertThat(repository.readSnapshot().contentHash()).isNotEqualTo(before);
    }

    @Test
    void readSnapshotKeepsANullRankWhenTheTableRowHasNone() {
        repository.replaceAll(plan(FIRST));
        // 테이블은 rank를 비워 둘 수 있으므로 SQL로 직접 넣어 확인한다.
        jdbc.sql("INSERT INTO instrument (kind, market, symbol) VALUES ('STOCK', 'US', 'FAKENR')").update();
        jdbc.sql("INSERT INTO etf_member (etf_id, member_id) "
                + "SELECT e.id, m.id FROM instrument e, instrument m "
                + "WHERE e.symbol = 'TEST01' AND m.symbol = 'FAKENR'").update();

        assertThat(repository.readSnapshot().memberships()).anySatisfy(m -> {
            assertThat(m.memberSymbol()).isEqualTo("FAKENR");
            assertThat(m.rank()).isNull();
        });
    }

    @Test
    void emptyPlanClearsBothTables() {
        repository.replaceAll(plan(FIRST));
        repository.replaceAll(HoldingsPlan.empty());

        assertThat(count("instrument")).isZero();
        assertThat(count("etf_member")).isZero();
    }

    @Test
    void failedSyncKeepsThePreviousState() {
        repository.replaceAll(plan(FIRST));
        // 없는 종목을 가리키는 구성 관계로 중간에 실패시킨다.
        HoldingsPlan broken = new HoldingsPlan(plan(FIRST).instruments(), java.util.List.of(
                new HoldingsPlan.Membership("KR", "TEST01", "US", "NOSUCH", null, 1)));

        assertThatThrownBy(() -> repository.replaceAll(broken)).isInstanceOf(RuntimeException.class);

        assertThat(count("etf_member")).isEqualTo(4);
        assertThat(repository.topTickers("TEST01")).containsExactly("FAKEA", "FAKE-B");
    }

    @Test
    void tableRulesRejectBadRows() {
        repository.replaceAll(plan(FIRST));

        assertThatThrownBy(() -> jdbc.sql("INSERT INTO instrument (kind, market, symbol, tracks) "
                + "VALUES ('STOCK', 'US', 'FAKEZ', 'X')").update()).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO instrument (kind, market, symbol) "
                + "VALUES ('STOCK', 'US', 'FAKEA')").update()).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("UPDATE etf_member SET weight = 100.5").update())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("UPDATE etf_member SET rank = 0").update())
                .isInstanceOf(RuntimeException.class);
    }

    private static long count(String table) {
        return jdbc.sql("SELECT count(*) FROM " + table).query(Long.class).single();
    }
}

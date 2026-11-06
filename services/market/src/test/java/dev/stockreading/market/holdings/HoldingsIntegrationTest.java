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

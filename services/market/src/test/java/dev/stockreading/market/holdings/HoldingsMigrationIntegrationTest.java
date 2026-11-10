package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.stockreading.market.DisposableDatabase;

/**
 * 옛 종목 표(instrument, etf_member)의 기존 행이 새 표(etf, stock, etf_stock_map)로 옮겨지는지 실제 Postgres로 확인한다.
 * 운영 표(market)를 건드리지 않도록 환경변수(MARKET_IT_DB_MIGRATION_SCHEMA)가 가리키는 별도 스키마에서만 돌며,
 * 그 스키마는 시작할 때 비우고(clean) 옛 버전(V9)까지 적용한 뒤 옛 행을 넣고 최신으로 올린다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_MIGRATION_SCHEMA", matches = "[a-z_]+")
class HoldingsMigrationIntegrationTest {

    private static Flyway flyway(String schema, String target) {
        return Flyway.configure()
                .dataSource(System.getenv("MARKET_IT_DB_URL"), System.getenv("MARKET_IT_DB_USER"),
                        System.getenv("MARKET_IT_DB_PASSWORD"))
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration")
                .cleanDisabled(false).target(target).load();
    }

    @Test
    void oldInstrumentRowsMoveToTheNewTablesKeepingIdsHistoryAndRawRows() {
        String schema = System.getenv("MARKET_IT_DB_MIGRATION_SCHEMA");
        // 운영 표를 비우지 않도록 시작 전에 막는다.
        assertThat(schema).isNotEqualTo("market").isNotEqualTo("public");
        JdbcClient jdbc = JdbcClient.create(new DriverManagerDataSource(System.getenv("MARKET_IT_DB_URL"),
                System.getenv("MARKET_IT_DB_USER"), System.getenv("MARKET_IT_DB_PASSWORD")));
        DisposableDatabase.require(jdbc);
        flyway(schema, "9").clean();
        flyway(schema, "9").migrate();
        String s = schema + ".";
        jdbc.sql("INSERT INTO " + s + "instrument (id, kind, market, symbol, name, tracks, relation) "
                + "VALUES (11, 'ETF', 'KR', 'OLD001', 'OLD ETF', 'OLD INDEX', 'HELD'), "
                + "(12, 'ETF', 'KR', 'OLD002', NULL, NULL, 'WATCH')").update();
        jdbc.sql("INSERT INTO " + s + "instrument (id, kind, market, symbol, name, sector, relation) "
                + "VALUES (21, 'STOCK', 'US', 'OLDA', 'OLD CORP A', 'OLD SECTOR', 'NONE'), "
                + "(22, 'STOCK', 'US', 'OLDB', NULL, NULL, 'HELD')").update();
        jdbc.sql("INSERT INTO " + s + "etf_member (etf_id, member_id, weight, rank) "
                + "VALUES (11, 21, 12.5, 1), (11, 22, NULL, 2), (12, 21, 30, 1), (11, 12, 5, 3)").update(); // 마지막은 ETF를 담은 행
        jdbc.sql("INSERT INTO " + s + "etf_member_history (id, etf_id, members, first_seen_on, last_seen_on) "
                + "VALUES (5, 11, '[]'::jsonb, DATE '2026-11-02', DATE '2026-11-09')").update();
        jdbc.sql("INSERT INTO " + s + "etf_member_raw (id, etf_id, base_date, http_status, body) "
                + "VALUES (7, 11, DATE '2026-11-06', 200, ' raw body ')").update();

        flyway(schema, "latest").migrate();

        assertThat(jdbc.sql("SELECT id || ':' || symbol || ':' || COALESCE(name, '-') || ':' || COALESCE(tracks, '-') "
                + "|| ':' || search_by_name FROM " + s + "etf ORDER BY id").query(String.class).list())
                .containsExactly("11:OLD001:OLD ETF:OLD INDEX:false", "12:OLD002:-:-:false");
        assertThat(jdbc.sql("SELECT id || ':' || symbol || ':' || COALESCE(sector, '-') FROM " + s
                + "stock ORDER BY id").query(String.class).list())
                .containsExactly("21:OLDA:OLD SECTOR", "22:OLDB:-");
        assertThat(jdbc.sql("SELECT etf_id || ':' || stock_id || ':' || COALESCE(weight::text, '-') || ':' || rank FROM "
                + s + "etf_stock_map ORDER BY etf_id, rank").query(String.class).list())
                .containsExactly("11:21:12.5000:1", "11:22:-:2", "12:21:30.0000:1");
        // ETF를 구성 종목으로 담은 옛 행(11 -> 12)은 새 표로 옮겨지지 않는다. 위 단언의 3줄이 전부다.
        assertThat(jdbc.sql("SELECT count(*) FROM " + s + "etf_stock_map WHERE stock_id = 12").query(Long.class).single())
                .isZero();
        // 옛 펀드 번호 설정 값은 어디에도 없으므로 이관 뒤에는 자동 조회 대상이 0건이다.
        assertThat(jdbc.sql("SELECT count(*) FROM " + s + "etf WHERE constituent_source_id IS NOT NULL")
                .query(Long.class).single()).isZero();
        // 이력과 원문은 이름만 바뀌고 행과 id가 그대로이며 새 etf 표를 가리킨다.
        assertThat(jdbc.sql("SELECT id || ':' || etf_id || ':' || last_seen_on FROM " + s + "etf_stock_history")
                .query(String.class).list()).containsExactly("5:11:2026-11-09");
        assertThat(jdbc.sql("SELECT id || ':' || etf_id || ':' || body FROM " + s + "etf_stock_raw")
                .query(String.class).list()).containsExactly("7:11: raw body ");
        // 새 행은 옛 id와 겹치지 않게 이어서 붙는다.
        assertThat(jdbc.sql("INSERT INTO " + s + "etf (market, symbol) VALUES ('KR', 'NEW001') RETURNING id")
                .query(Long.class).single()).isGreaterThan(12L);
        assertThat(jdbc.sql("INSERT INTO " + s + "stock (market, symbol) VALUES ('US', 'NEWA') RETURNING id")
                .query(Long.class).single()).isGreaterThan(22L);
        // 옛 표는 없어졌다.
        assertThat(jdbc.sql("SELECT count(*) FROM information_schema.tables WHERE table_schema = :schema "
                + "AND table_name IN ('instrument', 'etf_member', 'etf_member_history', 'etf_member_raw')")
                .param("schema", schema).query(Long.class).single()).isZero();
        // 이력은 etf 표의 행을 지우면 함께 지워진다(옛 표와 같은 규칙).
        jdbc.sql("DELETE FROM " + s + "etf WHERE id = 11").update();
        assertThat(jdbc.sql("SELECT count(*) FROM " + s + "etf_stock_history").query(Long.class).single()).isZero();
    }
}

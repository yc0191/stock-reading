package dev.stockreading.collector.members;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;

import dev.stockreading.collector.CollectorDb;

/**
 * 구성 종목 조회 대상(시세처리기의 etf·etf_stock_history·index_rebalance_schedule)을 수집기 계정으로 읽는 것을 실제 Postgres로 확인한다.
 * 가짜 ETF(COLL 접두어)를 시세처리기 계정으로 넣고, 수집기 계정이 읽은 결과와 쓰지 못함을 본다.
 */
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class DbMemberTargetsIntegrationTest {

    private static final LocalDate TODAY = LocalDate.parse("2026-12-08");

    private final JdbcClient market = CollectorDb.market();
    private final DbMemberTargets targets = new DbMemberTargets(CollectorDb.collector());

    @BeforeEach
    void seed() {
        CollectorDb.reset();
        market.sql("INSERT INTO etf (market, symbol, tracks, constituent_source_id) VALUES "
                + "('KR', 'COLL11', 'COLL INDEX', ' SRC11 '), ('KR', 'COLL12', 'COLL OTHER', 'SRC12'), "
                + "('KR', 'COLL13', NULL, 'SRC13'), ('KR', 'COLL14', 'COLL INDEX', NULL), "
                + "('KR', 'COLL15', 'COLL INDEX', '  '), ('US', 'COLL16', 'COLL INDEX', 'SRC16')").update();
        // 교체일이 든 달(12월)과 다른 달(1월) 일정
        market.sql("INSERT INTO index_rebalance_schedule (tracks, effective_date, note) VALUES "
                + "('COLL INDEX', DATE '2026-12-18', 'TEST'), ('COLL OTHER', DATE '2027-01-15', 'TEST')").update();
        // COLL11 은 두 번 반영되어 가장 최근 행(반영 시각 순)의 마지막 확인일을 읽어야 한다.
        history("COLL11", "2026-11-02", "2026-11-23", "2026-11-02T08:00:00+09:00");
        history("COLL11", "2026-11-30", "2026-12-07", "2026-11-30T08:00:00+09:00");
        history("COLL12", "2026-11-30", "2026-11-30", "2026-11-30T08:00:00+09:00");
    }

    private void history(String etf, String first, String last, String appliedAt) {
        market.sql("""
                        INSERT INTO etf_stock_history (etf_id, members, first_seen_on, last_seen_on, applied_at)
                        VALUES ((SELECT id FROM etf WHERE symbol = :e), CAST('[]' AS jsonb), CAST(:f AS date),
                                CAST(:l AS date), CAST(:a AS timestamptz))
                        """)
                .param("e", etf).param("f", first).param("l", last).param("a", appliedAt).update();
    }

    private List<MemberTargetsPort.Target> mine() {
        return targets.targets(TODAY).stream().filter(t -> t.symbol().startsWith("COLL")).toList();
    }

    @Test
    void readsKoreanEtfsWithASourceIdTheirLatestLastSeenDayAndTheRebalanceMonth() {
        assertThat(mine()).containsExactly(
                new MemberTargetsPort.Target("COLL11", "SRC11", LocalDate.parse("2026-12-07"), true),
                new MemberTargetsPort.Target("COLL12", "SRC12", LocalDate.parse("2026-11-30"), false),
                new MemberTargetsPort.Target("COLL13", "SRC13", null, false));
    }

    @Test
    void theRebalanceMonthIsTheCalendarMonthOfTheDayAsked() {
        assertThat(targets.targets(LocalDate.parse("2027-01-04")).stream()
                .filter(t -> t.symbol().startsWith("COLL")).map(MemberTargetsPort.Target::rebalanceMonth))
                .containsExactly(false, true, false);
        assertThat(targets.targets(LocalDate.parse("2026-11-30")).stream()
                .filter(t -> t.symbol().startsWith("COLL")).map(MemberTargetsPort.Target::rebalanceMonth))
                .containsExactly(false, false, false);
    }

    @Test
    void theCollectorAccountCannotWriteTheMarketTables() {
        JdbcClient collector = CollectorDb.collector();

        assertThatThrownBy(() -> collector.sql("UPDATE market.etf_stock_history SET last_seen_on = last_seen_on").update())
                .rootCause().hasMessageContaining("permission denied");
        assertThatThrownBy(() -> collector.sql("DELETE FROM market.index_rebalance_schedule").update())
                .rootCause().hasMessageContaining("permission denied");
    }
}

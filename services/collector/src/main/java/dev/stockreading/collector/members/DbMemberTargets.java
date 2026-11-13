package dev.stockreading.collector.members;

import java.time.LocalDate;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * 시세처리기의 ETF 표(market.etf), 구성 이력(market.etf_stock_history), 지수 교체 일정(market.index_rebalance_schedule)을
 * 수집기의 읽기 전용 계정으로 읽는다. 이력은 매일 바뀌므로 시작할 때가 아니라 조회할 때마다 읽는다.
 */
@Component
class DbMemberTargets implements MemberTargetsPort {

    private final JdbcClient jdbc;

    DbMemberTargets(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<Target> targets(LocalDate today) {
        LocalDate first = today.withDayOfMonth(1);
        // 이력의 가장 최근 행(반영 시각 순)이 현재 구성이다. 출처 번호가 없는 ETF는 구성 종목을 사람이 직접 고친다.
        return jdbc.sql("""
                        SELECT e.symbol, btrim(e.constituent_source_id) AS fund_id,
                               (SELECT h.last_seen_on FROM market.etf_stock_history h WHERE h.etf_id = e.id
                                ORDER BY h.applied_at DESC, h.id DESC LIMIT 1) AS last_seen_on,
                               EXISTS (SELECT 1 FROM market.index_rebalance_schedule r
                                       WHERE r.tracks = e.tracks AND r.effective_date >= :first
                                         AND r.effective_date < :next) AS rebalance_month
                        FROM market.etf e
                        WHERE e.market = 'KR' AND e.constituent_source_id IS NOT NULL
                          AND btrim(e.constituent_source_id) <> ''
                        ORDER BY e.symbol
                        """)
                .param("first", first).param("next", first.plusMonths(1))
                .query((rs, rowNum) -> new Target(rs.getString("symbol"), rs.getString("fund_id"),
                        rs.getObject("last_seen_on", LocalDate.class), rs.getBoolean("rebalance_month")))
                .list();
    }
}

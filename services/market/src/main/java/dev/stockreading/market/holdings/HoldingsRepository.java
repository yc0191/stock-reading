package dev.stockreading.market.holdings;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import dev.stockreading.market.event.HoldingsPayload;

/**
 * etf, stock, etf_stock_map 테이블에서 ETF 구성을 읽는다. 보유·관심 구분(relation)은 개인 데이터라 이 서비스의 표가 아니라
 * personal.active_portfolio(사용 중인 멤버의 포트폴리오 뷰)에 있고, 다른 스키마를 읽기 전용으로 읽어 계산한다(ADR-004 날짜 변경 줄).
 */
@Repository
class HoldingsRepository implements HoldingsPort {

    // 한 종목을 여러 멤버가 포트폴리오에 담을 수 있다. 한 명이라도 보유(HELD)면 HELD, 보유한 멤버 없이 지켜보기만 하면 WATCH,
    // 아무도 담지 않았으면 NONE이다. 사용 중이 아닌 멤버의 줄은 뷰가 거르므로 수집 대상과 같은 기준이다.
    private static final String RELATION = """
            CASE WHEN EXISTS (SELECT 1 FROM personal.active_portfolio p WHERE p.%1$s = t.id AND p.relation = 'HELD') THEN 'HELD'
                 WHEN EXISTS (SELECT 1 FROM personal.active_portfolio p WHERE p.%1$s = t.id) THEN 'WATCH'
                 ELSE 'NONE' END""";

    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;

    HoldingsRepository(JdbcClient jdbc, TransactionTemplate transaction) {
        this.jdbc = jdbc;
        this.transaction = transaction;
    }

    /**
     * 표의 전체 목록을 한 번에 읽는다. 읽는 중에 다른 쪽이 고치면 종목과 구성 관계가 어긋나게 읽히므로
     * 한 트랜잭션(REPEATABLE READ)에서 두 번 읽는다. personal 스키마가 아직 없으면 예외가 나고, 호출한 쪽이 다음 주기에 다시 시도한다.
     */
    HoldingsSnapshot readSnapshot() {
        TransactionTemplate readOnly = new TransactionTemplate(transaction.getTransactionManager());
        readOnly.setReadOnly(true);
        readOnly.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        return readOnly.execute(status -> {
            List<HoldingsPayload.Instrument> instruments = jdbc.sql(
                            "SELECT 'ETF' AS kind, t.market, t.symbol, " + RELATION.formatted("etf_id") + " AS relation, t.name FROM etf t"
                                    + " UNION ALL "
                                    + "SELECT 'STOCK', t.market, t.symbol, " + RELATION.formatted("stock_id") + ", t.name FROM stock t")
                    .query((rs, rowNum) -> new HoldingsPayload.Instrument(rs.getString("kind"),
                            rs.getString("market"), rs.getString("symbol"), rs.getString("relation"),
                            rs.getString("name")))
                    .list();
            List<HoldingsPayload.Membership> memberships = jdbc.sql("""
                            SELECT e.market AS etf_market, e.symbol AS etf_symbol, s.market AS member_market,
                                   s.symbol AS member_symbol, m.rank
                            FROM etf_stock_map m
                            JOIN etf e ON e.id = m.etf_id
                            JOIN stock s ON s.id = m.stock_id
                            """)
                    .query((rs, rowNum) -> new HoldingsPayload.Membership(rs.getString("etf_market"),
                            rs.getString("etf_symbol"), rs.getString("member_market"),
                            rs.getString("member_symbol"), (Integer) rs.getObject("rank")))
                    .list();
            return new HoldingsSnapshot(instruments, memberships);
        });
    }

    @Override
    public List<String> topTickers(String etfCode) {
        if (etfCode == null) {
            return List.of();
        }
        return jdbc.sql("""
                        SELECT s.symbol FROM etf_stock_map m
                        JOIN etf e ON e.id = m.etf_id
                        JOIN stock s ON s.id = m.stock_id
                        WHERE e.market = 'KR' AND e.symbol = :etf AND s.market = 'US'
                        ORDER BY m.rank, s.symbol
                        """)
                .param("etf", etfCode).query(String.class).list();
    }

    @Override
    public List<TrackedEtf> trackedEtfs() {
        return jdbc.sql("""
                        SELECT e.symbol, CASE WHEN bool_or(p.relation = 'HELD') THEN 'HELD' ELSE 'WATCH' END AS relation
                        FROM etf e
                        JOIN personal.active_portfolio p ON p.etf_id = e.id
                        WHERE e.market = 'KR'
                        GROUP BY e.symbol
                        ORDER BY e.symbol
                        """)
                .query((rs, rowNum) -> new TrackedEtf(rs.getString("symbol"), rs.getString("relation"))).list();
    }

    @Override
    public Optional<EtfInfo> etfInfo(String etfCode) {
        if (etfCode == null) {
            return Optional.empty();
        }
        Optional<Long> etfId = jdbc.sql("SELECT id FROM etf WHERE market = 'KR' AND symbol = :etf")
                .param("etf", etfCode).query(Long.class).optional();
        if (etfId.isEmpty()) {
            return Optional.empty();
        }
        List<EtfInfo.Member> members = jdbc.sql("""
                        SELECT s.market, s.symbol, s.name, m.weight, m.rank FROM etf_stock_map m
                        JOIN stock s ON s.id = m.stock_id
                        WHERE m.etf_id = :id
                        ORDER BY m.rank, s.market, s.symbol
                        """)
                .param("id", etfId.get())
                .query((rs, rowNum) -> new EtfInfo.Member(rs.getString("market"), rs.getString("symbol"),
                        rs.getString("name"), rs.getObject("weight", BigDecimal.class), rs.getInt("rank")))
                .list();
        // ETF 표에는 업종 칸이 없어 sector는 항상 비어 있다.
        return jdbc.sql("SELECT name, tracks FROM etf WHERE id = :id").param("id", etfId.get())
                .query((rs, rowNum) -> new EtfInfo(rs.getString("name"), rs.getString("tracks"), null, members))
                .optional();
    }
}

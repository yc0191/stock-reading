package dev.stockreading.market.constituent;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;


/**
 * etf, stock, etf_stock_map 테이블에서 ETF 구성을 읽는다. 포트폴리오에 담긴 ETF는 개인 데이터라 이 서비스의 표가 아니라
 * personal.active_portfolio(사용 중인 멤버의 포트폴리오 뷰)에서 읽기 전용으로 읽는다(ADR-004 날짜 변경 줄). 보유·관심 구분은 읽지 않는다.
 */
@Repository
class HoldingsRepository implements HoldingsPort {

    private final JdbcClient jdbc;

    HoldingsRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
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
    public List<String> trackedEtfs() {
        return jdbc.sql("""
                        SELECT DISTINCT e.symbol
                        FROM etf e
                        JOIN personal.active_portfolio p ON p.etf_id = e.id
                        WHERE e.market = 'KR'
                        ORDER BY e.symbol
                        """)
                .query(String.class).list();
    }

    @Override
    public Optional<String> stockName(String code) {
        if (code == null) {
            return Optional.empty();
        }
        return jdbc.sql("SELECT name FROM stock WHERE market = 'KR' AND symbol = :code")
                .param("code", code).query(String.class).optional().filter(name -> !name.isBlank());
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

package dev.stockreading.market.holdings;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

/** instrument와 etf_member 테이블을 파일의 목표 상태와 똑같이 맞추고, 맞춘 테이블에서 ETF 구성을 읽는다. */
@Repository
class HoldingsRepository implements HoldingsPort {

    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;

    HoldingsRepository(JdbcClient jdbc, TransactionTemplate transaction) {
        this.jdbc = jdbc;
        this.transaction = transaction;
    }

    /**
     * 목표 상태와 똑같이 맞춘다: 있는 종목은 값을 갱신하고, 목표에 없는 종목과 구성 관계는 지운다.
     * 한 트랜잭션이라 중간에 실패하면 이전 상태가 그대로 남는다.
     */
    void replaceAll(HoldingsPlan plan) {
        transaction.executeWithoutResult(status -> {
            jdbc.sql("DELETE FROM etf_member").update();
            List<Long> keptIds = new ArrayList<>();
            for (HoldingsPlan.Instrument i : plan.instruments()) {
                keptIds.add(jdbc.sql("""
                                INSERT INTO instrument (kind, market, symbol, name, tracks, sector, relation, updated_at)
                                VALUES (:kind, :market, :symbol, :name, :tracks, :sector, :relation, now())
                                ON CONFLICT (market, symbol) DO UPDATE SET kind = EXCLUDED.kind, name = EXCLUDED.name,
                                    tracks = EXCLUDED.tracks, sector = EXCLUDED.sector, relation = EXCLUDED.relation,
                                    updated_at = now()
                                RETURNING id
                                """)
                        .param("kind", i.kind()).param("market", i.market()).param("symbol", i.symbol())
                        .param("name", i.name()).param("tracks", i.tracks()).param("sector", i.sector())
                        .param("relation", i.relation())
                        .query(Long.class).single());
            }
            if (keptIds.isEmpty()) {
                jdbc.sql("DELETE FROM instrument").update();
            } else {
                jdbc.sql("DELETE FROM instrument WHERE id NOT IN (:ids)").param("ids", keptIds).update();
            }
            for (HoldingsPlan.Membership m : plan.memberships()) {
                jdbc.sql("""
                                INSERT INTO etf_member (etf_id, member_id, weight, rank)
                                VALUES ((SELECT id FROM instrument WHERE market = :etfMarket AND symbol = :etfSymbol),
                                        (SELECT id FROM instrument WHERE market = :memberMarket AND symbol = :memberSymbol),
                                        :weight, :rank)
                                """)
                        .param("etfMarket", m.etfMarket()).param("etfSymbol", m.etfSymbol())
                        .param("memberMarket", m.memberMarket()).param("memberSymbol", m.memberSymbol())
                        .param("weight", m.weight()).param("rank", m.rank())
                        .update();
            }
        });
    }

    @Override
    public List<String> topTickers(String etfCode) {
        if (etfCode == null) {
            return List.of();
        }
        return jdbc.sql("""
                        SELECT m.symbol FROM etf_member em
                        JOIN instrument e ON e.id = em.etf_id
                        JOIN instrument m ON m.id = em.member_id
                        WHERE e.kind = 'ETF' AND e.market = 'KR' AND e.symbol = :etf AND m.market = 'US'
                        ORDER BY em.rank, m.symbol
                        """)
                .param("etf", etfCode).query(String.class).list();
    }

    @Override
    public List<TrackedEtf> trackedEtfs() {
        return jdbc.sql("""
                        SELECT symbol, relation FROM instrument
                        WHERE kind = 'ETF' AND market = 'KR' AND relation IN ('HELD', 'WATCH')
                        ORDER BY symbol
                        """)
                .query((rs, rowNum) -> new TrackedEtf(rs.getString("symbol"), rs.getString("relation"))).list();
    }

    @Override
    public Optional<EtfInfo> etfInfo(String etfCode) {
        if (etfCode == null) {
            return Optional.empty();
        }
        Optional<Long> etfId = jdbc.sql("SELECT id FROM instrument WHERE kind = 'ETF' AND market = 'KR' AND symbol = :etf")
                .param("etf", etfCode).query(Long.class).optional();
        if (etfId.isEmpty()) {
            return Optional.empty();
        }
        List<EtfInfo.Member> members = jdbc.sql("""
                        SELECT m.market, m.symbol, m.name, em.weight, em.rank FROM etf_member em
                        JOIN instrument m ON m.id = em.member_id
                        WHERE em.etf_id = :id
                        ORDER BY em.rank, m.market, m.symbol
                        """)
                .param("id", etfId.get())
                .query((rs, rowNum) -> new EtfInfo.Member(rs.getString("market"), rs.getString("symbol"),
                        rs.getString("name"), rs.getObject("weight", BigDecimal.class), rs.getInt("rank")))
                .list();
        return jdbc.sql("SELECT name, tracks, sector FROM instrument WHERE id = :id").param("id", etfId.get())
                .query((rs, rowNum) -> new EtfInfo(rs.getString("name"), rs.getString("tracks"),
                        rs.getString("sector"), members))
                .optional();
    }
}

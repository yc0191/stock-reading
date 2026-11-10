package dev.stockreading.market.members;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/** 구성 종목 이력(etf_member_history)과 교체 일정을 읽고 쓰며, 구성이 바뀌면 종목 테이블의 구성(etf_member)도 맞춘다. */
@Repository
class MemberRepository implements MemberChangesPort {

    private static final TypeReference<List<Member>> MEMBER_LIST = new TypeReference<>() {
    };
    // 이력은 구성이 바뀔 때만 늘지만, 읽을 때 무한정 가져오지 않도록 상한을 둔다. 판 범위 안 변경은 이보다 훨씬 적다.
    private static final int HISTORY_READ_LIMIT = 50;

    private final JdbcClient jdbc;
    private final TransactionTemplate transaction;
    private final ObjectMapper mapper;

    MemberRepository(JdbcClient jdbc, TransactionTemplate transaction, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.mapper = mapper;
    }

    /** 자동 조회 대상 후보: 관계가 HELD 또는 WATCH인 국내 ETF. */
    List<Target> targets() {
        return jdbc.sql("""
                        SELECT id, symbol, tracks FROM instrument
                        WHERE kind = 'ETF' AND market = 'KR' AND relation IN ('HELD', 'WATCH')
                        ORDER BY symbol
                        """)
                .query((rs, rowNum) -> new Target(rs.getLong("id"), rs.getString("symbol"), rs.getString("tracks")))
                .list();
    }

    record Target(long etfId, String symbol, String tracks) {
    }

    /** today가 든 달에 이 지수의 교체일이 있으면 true. */
    boolean inRebalanceMonth(String tracks, LocalDate today) {
        if (tracks == null) {
            return false;
        }
        LocalDate first = today.withDayOfMonth(1);
        return jdbc.sql("""
                        SELECT EXISTS (SELECT 1 FROM index_rebalance_schedule
                                       WHERE tracks = :tracks AND effective_date >= :first AND effective_date < :next)
                        """)
                .param("tracks", tracks).param("first", first).param("next", first.plusMonths(1))
                .query(Boolean.class).single();
    }

    /** 가장 최근 이력 행. 아직 없으면 빈 값. */
    Optional<Latest> latest(long etfId) {
        return jdbc.sql("""
                        SELECT id, members, last_seen_on FROM etf_member_history
                        WHERE etf_id = :etf ORDER BY applied_at DESC, id DESC LIMIT 1
                        """)
                .param("etf", etfId)
                .query((rs, rowNum) -> new Latest(rs.getLong("id"), readMembers(rs.getString("members")),
                        rs.getObject("last_seen_on", LocalDate.class)))
                .optional();
    }

    record Latest(long id, List<Member> members, LocalDate lastSeenOn) {
    }

    /**
     * 구성이 그대로일 때: 이력은 마지막으로 확인한 날만 고치고, 종목 테이블의 비중(etf_member.weight)은 새로 받은 값으로 맞춘다.
     * 비중은 구성 비교와 holdings 해시에 들어가지 않으므로 고쳐도 변경으로 보지 않는다.
     */
    void touch(long historyId, long etfId, List<Member> members, LocalDate today) {
        transaction.executeWithoutResult(status -> {
            jdbc.sql("UPDATE etf_member_history SET last_seen_on = :today WHERE id = :id AND last_seen_on < :today")
                    .param("today", today).param("id", historyId).update();
            for (Member m : members) {
                jdbc.sql("""
                                UPDATE etf_member SET weight = :weight
                                WHERE etf_id = :etf
                                  AND member_id = (SELECT id FROM instrument WHERE market = :market AND symbol = :symbol)
                                """)
                        .param("weight", m.weight() == null ? null : new BigDecimal(m.weight()))
                        .param("etf", etfId).param("market", m.market()).param("symbol", m.symbol()).update();
            }
        });
    }

    /** 받은 응답 원문을 그대로 남긴다(실패한 응답 포함). 이력(etf_member_history)은 이 원문에서 계산한 결과다. */
    void saveRaw(long etfId, LocalDate baseDate, int httpStatus, String body) {
        jdbc.sql("""
                        INSERT INTO etf_member_raw (etf_id, base_date, http_status, body)
                        VALUES (:etf, :baseDate, :status, :body)
                        """)
                .param("etf", etfId).param("baseDate", baseDate).param("status", httpStatus).param("body", body)
                .update();
    }

    /**
     * 새 구성을 이력에 넣고 종목 테이블의 이 ETF 구성을 같게 맞춘다. 한 트랜잭션이라 중간에 실패하면 이전 상태가 남는다.
     * 관리 중인 종목(HELD·WATCH이거나 이름이 이미 있는 행)의 이름은 출처 이름으로 바꾸지 않는다.
     * 구성 종목 중 처음 보는 종목은 구성으로만 아는 종목(NONE)으로 넣고, 어느 ETF에도 담기지 않게 된 NONE 종목은 지운다.
     */
    void record(long etfId, List<Member> members, LocalDate today) {
        transaction.executeWithoutResult(status -> {
            jdbc.sql("""
                            INSERT INTO etf_member_history (etf_id, members, first_seen_on, last_seen_on)
                            VALUES (:etf, CAST(:members AS jsonb), :today, :today)
                            """)
                    .param("etf", etfId).param("members", mapper.writeValueAsString(members)).param("today", today)
                    .update();
            jdbc.sql("DELETE FROM etf_member WHERE etf_id = :etf").param("etf", etfId).update();
            for (Member m : members) {
                long memberId = jdbc.sql("""
                                INSERT INTO instrument (kind, market, symbol, name, relation, updated_at)
                                VALUES ('STOCK', :market, :symbol, :name, 'NONE', now())
                                ON CONFLICT (market, symbol) DO UPDATE
                                    SET name = CASE WHEN instrument.relation = 'NONE' AND instrument.name IS NULL
                                                    THEN EXCLUDED.name ELSE instrument.name END,
                                        updated_at = now()
                                RETURNING id
                                """)
                        .param("market", m.market()).param("symbol", m.symbol()).param("name", m.name())
                        .query(Long.class).single();
                jdbc.sql("INSERT INTO etf_member (etf_id, member_id, weight, rank) VALUES (:etf, :member, :weight, :rank)")
                        .param("etf", etfId).param("member", memberId)
                        .param("weight", m.weight() == null ? null : new BigDecimal(m.weight()))
                        .param("rank", m.rank()).update();
            }
            jdbc.sql("""
                            DELETE FROM instrument
                            WHERE kind = 'STOCK' AND relation = 'NONE'
                              AND NOT EXISTS (SELECT 1 FROM etf_member WHERE member_id = instrument.id)
                            """)
                    .update();
        });
    }

    @Override
    public Optional<MemberChanges> changes(String etfCode, OffsetDateTime since, OffsetDateTime asOf) {
        List<MemberChangeFinder.Row> rows = jdbc.sql("""
                        SELECT h.members, h.first_seen_on, h.last_seen_on, h.applied_at FROM etf_member_history h
                        JOIN instrument e ON e.id = h.etf_id
                        WHERE e.kind = 'ETF' AND e.market = 'KR' AND e.symbol = :etf AND h.applied_at <= :asOf
                        ORDER BY h.applied_at DESC, h.id DESC LIMIT :limit
                        """)
                .param("etf", etfCode).param("asOf", asOf).param("limit", HISTORY_READ_LIMIT)
                .query((rs, rowNum) -> new MemberChangeFinder.Row(rs.getObject("applied_at", OffsetDateTime.class),
                        rs.getObject("first_seen_on", LocalDate.class), rs.getObject("last_seen_on", LocalDate.class),
                        readMembers(rs.getString("members"))))
                .list();
        return MemberChangeFinder.find(rows, since);
    }

    private List<Member> readMembers(String json) {
        return mapper.readValue(json, MEMBER_LIST);
    }
}

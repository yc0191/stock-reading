package dev.stockreading.market.members;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/** 구성 종목 원문(etf_stock_raw)과 이력(etf_stock_history)을 읽고 쓰며, 구성이 바뀌면 ETF 구성 연결 표(etf_stock_map)도 맞춘다. */
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

    /** 국내 ETF 종목코드로 표의 id를 찾는다. 없으면 빈 값. */
    Optional<Long> etfId(String symbol) {
        return jdbc.sql("SELECT id FROM etf WHERE market = 'KR' AND symbol = :symbol")
                .param("symbol", symbol).query(Long.class).optional();
    }

    /** 가장 최근 이력 행. 아직 없으면 빈 값. */
    Optional<Latest> latest(long etfId) {
        return jdbc.sql("""
                        SELECT id, members, last_seen_on FROM etf_stock_history
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
     * 구성이 그대로일 때: 이력은 마지막으로 확인한 날만 고치고, 연결 표의 비중(etf_stock_map.weight)은 새로 받은 값으로 맞춘다.
     * 비중은 구성 비교와 holdings 해시에 들어가지 않으므로 고쳐도 변경으로 보지 않는다.
     */
    void touch(long historyId, long etfId, List<Member> members, LocalDate today) {
        transaction.executeWithoutResult(status -> {
            jdbc.sql("UPDATE etf_stock_history SET last_seen_on = :today WHERE id = :id AND last_seen_on < :today")
                    .param("today", today).param("id", historyId).update();
            for (Member m : members) {
                jdbc.sql("""
                                UPDATE etf_stock_map SET weight = :weight
                                WHERE etf_id = :etf
                                  AND stock_id = (SELECT id FROM stock WHERE market = :market AND symbol = :symbol)
                                """)
                        .param("weight", m.weight() == null ? null : new BigDecimal(m.weight()))
                        .param("etf", etfId).param("market", m.market()).param("symbol", m.symbol()).update();
            }
        });
    }

    /**
     * 수집기가 받은 응답 원문을 그대로 남긴다(실패한 응답 포함). 받은 시각은 수집기가 응답을 받은 시각이다.
     * 이력(etf_stock_history)은 이 원문에서 계산한 결과다.
     *
     * @return 처음 받은 이벤트면 true. 같은 eventId가 이미 있으면 남기지 않고 false
     */
    boolean saveRaw(UUID eventId, long etfId, LocalDate baseDate, int httpStatus, String body, OffsetDateTime fetchedAt) {
        return jdbc.sql("""
                        INSERT INTO etf_stock_raw (event_id, etf_id, base_date, http_status, body, fetched_at)
                        VALUES (:eventId, :etf, :baseDate, :status, :body, :fetchedAt)
                        ON CONFLICT (event_id) DO NOTHING
                        """)
                .param("eventId", eventId).param("etf", etfId).param("baseDate", baseDate).param("status", httpStatus)
                .param("body", body).param("fetchedAt", fetchedAt)
                .update() == 1;
    }

    /**
     * 새 구성을 이력에 넣고 이 ETF의 연결 표(etf_stock_map) 줄을 같게 맞춘다. 한 트랜잭션이라 중간에 실패하면 이전 상태가 남는다.
     * 연결 줄만 고치고 stock 행은 지우지 않는다(개인 포트폴리오가 주식 행을 가리킬 수 있다). 이미 이름이 있는 주식의 이름은
     * 출처 이름으로 바꾸지 않고, 처음 보는 구성 종목은 새 stock 행으로 넣는다.
     */
    void record(long etfId, List<Member> members, LocalDate today) {
        transaction.executeWithoutResult(status -> {
            jdbc.sql("""
                            INSERT INTO etf_stock_history (etf_id, members, first_seen_on, last_seen_on)
                            VALUES (:etf, CAST(:members AS jsonb), :today, :today)
                            """)
                    .param("etf", etfId).param("members", mapper.writeValueAsString(members)).param("today", today)
                    .update();
            jdbc.sql("DELETE FROM etf_stock_map WHERE etf_id = :etf").param("etf", etfId).update();
            for (Member m : members) {
                long stockId = jdbc.sql("""
                                INSERT INTO stock (market, symbol, name)
                                VALUES (:market, :symbol, :name)
                                ON CONFLICT (market, symbol) DO UPDATE
                                    SET name = COALESCE(stock.name, EXCLUDED.name),
                                        updated_at = CASE WHEN stock.name IS NULL AND EXCLUDED.name IS NOT NULL
                                                          THEN now() ELSE stock.updated_at END
                                RETURNING id
                                """)
                        .param("market", m.market()).param("symbol", m.symbol()).param("name", m.name())
                        .query(Long.class).single();
                jdbc.sql("INSERT INTO etf_stock_map (etf_id, stock_id, weight, rank) VALUES (:etf, :stock, :weight, :rank)")
                        .param("etf", etfId).param("stock", stockId)
                        .param("weight", m.weight() == null ? null : new BigDecimal(m.weight()))
                        .param("rank", m.rank()).update();
            }
        });
    }

    @Override
    public Optional<MemberChanges> changes(String etfCode, OffsetDateTime since, OffsetDateTime asOf) {
        List<MemberChangeFinder.Row> rows = jdbc.sql("""
                        SELECT h.members, h.first_seen_on, h.last_seen_on, h.applied_at FROM etf_stock_history h
                        JOIN etf e ON e.id = h.etf_id
                        WHERE e.market = 'KR' AND e.symbol = :etf AND h.applied_at <= :asOf
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

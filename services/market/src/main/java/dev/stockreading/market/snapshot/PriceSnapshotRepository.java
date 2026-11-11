package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class PriceSnapshotRepository {

    private final JdbcClient jdbc;

    PriceSnapshotRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 멱등 삽입: 다시 전달된 이벤트(같은 eventId)는 무시한다. 파티션 테이블이라 유니크 키에 as_of가 들어간다. */
    boolean insertIfAbsent(UUID eventId, String symbol, OffsetDateTime asOf, long price, String rawJson) {
        int rows = jdbc.sql("""
                        INSERT INTO price_snapshot (event_id, symbol, as_of, price, raw)
                        VALUES (:eventId, :symbol, :asOf, :price, CAST(CAST(:raw AS text) AS jsonb))
                        ON CONFLICT (event_id, as_of) DO NOTHING
                        """)
                .param("eventId", eventId)
                .param("symbol", symbol)
                .param("asOf", asOf)
                .param("price", price)
                .param("raw", rawJson)
                .update();
        return rows == 1;
    }

    /**
     * [from, to) 구간의 스냅샷을 시간 순으로 돌려준다. 날짜 파티션 가지치기를 위해 항상 as_of 범위 조건을 쓴다.
     * firstTradeFields(점으로 중첩 경로)의 값이 모두 0이면 beforeFirstTrade로 표시한다. 칸이 없거나 숫자가 아니면 표시하지 않고,
     * 목록이 비면 표시하지 않는다. 한 스냅샷씩 원문을 자바로 읽지 않으려고 데이터베이스에서 판별한다.
     */
    List<PricePoint> findBetween(String symbol, OffsetDateTime from, OffsetDateTime to, List<String> firstTradeFields) {
        return select(symbol, from, to, firstTradeFields, "ASC", Integer.MAX_VALUE);
    }

    /** [from, to) 구간에서 to에 가장 가까운 limit건을 시간 순으로 돌려준다. */
    List<PricePoint> findLatestBetween(
            String symbol, OffsetDateTime from, OffsetDateTime to, int limit, List<String> firstTradeFields) {
        List<PricePoint> latestFirst = select(symbol, from, to, firstTradeFields, "DESC", limit);
        List<PricePoint> points = new ArrayList<>(latestFirst);
        Collections.reverse(points);
        return points;
    }

    private List<PricePoint> select(
            String symbol, OffsetDateTime from, OffsetDateTime to, List<String> firstTradeFields, String order, int limit) {
        StringBuilder before = new StringBuilder();
        for (int i = 0; i < firstTradeFields.size(); i++) {
            before.append(i == 0 ? "" : " AND ")
                    .append("COALESCE(btrim(raw #>> string_to_array(CAST(:f").append(i).append(" AS text), '.')) ~ '^0+(\\.0*)?$', false)");
        }
        JdbcClient.StatementSpec spec = jdbc.sql("""
                        SELECT as_of, price, %s AS before_first_trade FROM price_snapshot
                        WHERE symbol = :symbol AND as_of >= :from AND as_of < :to
                        ORDER BY as_of %s LIMIT :limit
                        """.formatted(before.isEmpty() ? "false" : before, order))
                .param("symbol", symbol)
                .param("from", from)
                .param("to", to)
                .param("limit", limit);
        for (int i = 0; i < firstTradeFields.size(); i++) {
            spec = spec.param("f" + i, firstTradeFields.get(i));
        }
        return spec
                .query((rs, rowNum) -> new PricePoint(
                        rs.getObject("as_of", OffsetDateTime.class), rs.getLong("price"), rs.getBoolean("before_first_trade")))
                .list();
    }
}

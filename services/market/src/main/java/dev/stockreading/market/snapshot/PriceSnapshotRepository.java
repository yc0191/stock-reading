package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
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

    /** [from, to) 구간의 스냅샷을 시간 순으로 돌려준다. 날짜 파티션 가지치기를 위해 항상 as_of 범위 조건을 쓴다. */
    List<PricePoint> findBetween(String symbol, OffsetDateTime from, OffsetDateTime to) {
        return jdbc.sql("""
                        SELECT as_of, price FROM price_snapshot
                        WHERE symbol = :symbol AND as_of >= :from AND as_of < :to
                        ORDER BY as_of
                        """)
                .param("symbol", symbol)
                .param("from", from)
                .param("to", to)
                .query((rs, rowNum) -> new PricePoint(rs.getObject("as_of", OffsetDateTime.class), rs.getLong("price")))
                .list();
    }
}

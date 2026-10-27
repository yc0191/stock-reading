package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
import java.util.Optional;
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

    Optional<Long> findPreviousPrice(String symbol, OffsetDateTime before) {
        return jdbc.sql("""
                        SELECT price FROM price_snapshot
                        WHERE symbol = :symbol AND as_of < :asOf
                        ORDER BY as_of DESC
                        LIMIT 1
                        """)
                .param("symbol", symbol)
                .param("asOf", before)
                .query(Long.class)
                .optional();
    }
}

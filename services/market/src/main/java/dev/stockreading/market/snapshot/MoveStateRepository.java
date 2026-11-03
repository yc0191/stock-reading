package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class MoveStateRepository {

    private final JdbcClient jdbc;

    MoveStateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    Optional<MoveState> find(String symbol, DetectionType type) {
        return jdbc.sql("""
                        SELECT last_published_price, move_started_at FROM move_state
                        WHERE symbol = :symbol AND detection_type = :type
                        """)
                .param("symbol", symbol)
                .param("type", type.wire())
                .query((rs, rowNum) -> new MoveState(
                        rs.getLong("last_published_price"), rs.getObject("move_started_at", OffsetDateTime.class)))
                .optional();
    }

    void save(String symbol, DetectionType type, long price, OffsetDateTime moveStartedAt, OffsetDateTime publishedAt) {
        jdbc.sql("""
                        INSERT INTO move_state (symbol, detection_type, last_published_price, move_started_at, last_published_at)
                        VALUES (:symbol, :type, :price, :startedAt, :publishedAt)
                        ON CONFLICT (symbol, detection_type) DO UPDATE
                        SET last_published_price = EXCLUDED.last_published_price,
                            move_started_at = EXCLUDED.move_started_at,
                            last_published_at = EXCLUDED.last_published_at
                        """)
                .param("symbol", symbol)
                .param("type", type.wire())
                .param("price", price)
                .param("startedAt", moveStartedAt)
                .param("publishedAt", publishedAt)
                .update();
    }

    void delete(String symbol, DetectionType type) {
        jdbc.sql("DELETE FROM move_state WHERE symbol = :symbol AND detection_type = :type")
                .param("symbol", symbol)
                .param("type", type.wire())
                .update();
    }
}

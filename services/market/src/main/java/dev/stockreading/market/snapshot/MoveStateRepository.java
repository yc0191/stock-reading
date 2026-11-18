package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class MoveStateRepository {

    /**
     * 종목 단위로 마지막 발행(유형 무관)을 기억하는 행의 detection_type 값. 감지 조건이 풀려도 지우지 않는다.
     * 유형 칸에 CHECK가 없어서(V6) 마이그레이션 없이 같은 테이블에 둔다.
     */
    private static final String LAST_PUBLISH = "lastPublish";

    private final JdbcClient jdbc;

    MoveStateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    Optional<MoveState> find(String symbol, DetectionType type) {
        return jdbc.sql("""
                        SELECT last_published_price, move_started_at, last_published_at FROM move_state
                        WHERE symbol = :symbol AND detection_type = :type
                        """)
                .param("symbol", symbol)
                .param("type", type.wire())
                .query((rs, rowNum) -> new MoveState(
                        rs.getLong("last_published_price"), rs.getObject("move_started_at", OffsetDateTime.class),
                        rs.getObject("last_published_at", OffsetDateTime.class)))
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

    /**
     * 감지 조건이 풀렸음을 남긴다. 그날 재발행 판단에 마지막 발행 가격이 계속 필요해 행을 지우지 않고,
     * 마이그레이션 없이 시작 시각 칸을 풀린 시각으로 바꿔 표시한다(시작 시각이 마지막 발행 시각보다 늦으면 끝난 변동).
     * 이미 끝난 행과 풀린 시각이 마지막 발행보다 이른 경우(지난 시세 재생)는 그대로 둔다.
     */
    void markEnded(String symbol, DetectionType type, OffsetDateTime endedAt) {
        jdbc.sql("""
                        UPDATE move_state SET move_started_at = :endedAt
                        WHERE symbol = :symbol AND detection_type = :type
                          AND move_started_at <= last_published_at AND last_published_at < :endedAt
                        """)
                .param("symbol", symbol)
                .param("type", type.wire())
                .param("endedAt", endedAt)
                .update();
    }

    void delete(String symbol, DetectionType type) {
        jdbc.sql("DELETE FROM move_state WHERE symbol = :symbol AND detection_type = :type")
                .param("symbol", symbol)
                .param("type", type.wire())
                .update();
    }

    Optional<LastPublish> findLastPublish(String symbol) {
        return jdbc.sql("""
                        SELECT last_published_price, last_published_at FROM move_state
                        WHERE symbol = :symbol AND detection_type = :type
                        """)
                .param("symbol", symbol)
                .param("type", LAST_PUBLISH)
                .query((rs, rowNum) -> new LastPublish(
                        rs.getLong("last_published_price"), rs.getObject("last_published_at", OffsetDateTime.class)))
                .optional();
    }

    void saveLastPublish(String symbol, long price, OffsetDateTime publishedAt) {
        jdbc.sql("""
                        INSERT INTO move_state (symbol, detection_type, last_published_price, move_started_at, last_published_at)
                        VALUES (:symbol, :type, :price, :publishedAt, :publishedAt)
                        ON CONFLICT (symbol, detection_type) DO UPDATE
                        SET last_published_price = EXCLUDED.last_published_price,
                            move_started_at = EXCLUDED.move_started_at,
                            last_published_at = EXCLUDED.last_published_at
                        """)
                .param("symbol", symbol)
                .param("type", LAST_PUBLISH)
                .param("price", price)
                .param("publishedAt", publishedAt)
                .update();
    }
}

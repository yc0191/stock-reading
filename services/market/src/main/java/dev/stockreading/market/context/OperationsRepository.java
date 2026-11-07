package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** 시세처리기가 저장한 수신 현황을 세기만 한다. 종목은 읽지 않고 쓰기 경로도 없다. */
@Repository
class OperationsRepository {

    private final JdbcClient jdbc;

    OperationsRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 수집 종류별 [처음, 마지막] 받은 시각과 건수. 받은 자료가 있는 종류만 돌려준다. 범위는 양끝 포함이다.
     * 조회 자료 테이블에는 어느 수집이 받았는지 칸이 없어 받은 시각의 한국 시간대로 가른다: 09:00 전은 장 시작 전,
     * 09:00~16:00 전은 장중, 16:00 이후는 장 마감 후. 해외 일봉은 장 시작 전에만, 국내 일봉은 장 마감 후에만 받는다.
     */
    List<CollectionRow> collections(OffsetDateTime since, OffsetDateTime asOf) {
        return jdbc.sql("""
                        SELECT kind, MIN(at) AS first_at, MAX(at) AS last_at, COUNT(*) AS cnt
                        FROM (
                            SELECT 'quote-1s' AS kind, received_at AS at FROM price_snapshot
                            WHERE received_at >= :since AND received_at <= :asOf
                              AND as_of >= :asOfLow AND as_of <= :asOfHigh
                            UNION ALL
                            SELECT 'pre-open', fetched_at FROM overseas_daily_price
                            WHERE fetched_at >= :since AND fetched_at <= :asOf
                            UNION ALL
                            SELECT 'post-close', fetched_at FROM daily_price
                            WHERE fetched_at >= :since AND fetched_at <= :asOf
                            UNION ALL
                            SELECT CASE WHEN tod < TIME '09:00' THEN 'pre-open'
                                        WHEN tod < TIME '16:00' THEN 'intraday'
                                        ELSE 'post-close' END, fetched_at
                            FROM (
                                SELECT fetched_at, CAST(fetched_at AT TIME ZONE 'Asia/Seoul' AS time) AS tod
                                FROM world_index
                                WHERE fetched_at >= :since AND fetched_at <= :asOf
                                UNION ALL
                                SELECT fetched_at, CAST(fetched_at AT TIME ZONE 'Asia/Seoul' AS time)
                                FROM exchange_rate_summary
                                WHERE fetched_at >= :since AND fetched_at <= :asOf
                                UNION ALL
                                SELECT fetched_at, CAST(fetched_at AT TIME ZONE 'Asia/Seoul' AS time)
                                FROM market_summary
                                WHERE fetched_at >= :since AND fetched_at <= :asOf
                            ) insight
                        ) received
                        GROUP BY kind
                        """)
                .param("since", since)
                .param("asOf", asOf)
                // 시세 테이블은 as_of로 나뉘어 있어 받은 시각만으로는 모든 파티션을 읽는다. 저장 지연을 넉넉히 둔 범위로 좁힌다.
                .param("asOfLow", since.minusHours(1))
                .param("asOfHigh", asOf.plusHours(1))
                .query((rs, rowNum) -> new CollectionRow(rs.getString("kind"),
                        rs.getObject("first_at", OffsetDateTime.class), rs.getObject("last_at", OffsetDateTime.class),
                        rs.getLong("cnt")))
                .list();
    }

    /** 시세 실패 종류별 건수. TICK_SKIPPED는 사유를 나누어 센다. 건수가 있는 종류만 돌려준다. */
    Map<String, Integer> quoteFailures(OffsetDateTime since, OffsetDateTime asOf) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        jdbc.sql("""
                        SELECT CASE WHEN failure_type = 'TICK_SKIPPED' THEN
                                        CASE exception_type WHEN 'RateLimited' THEN 'TICK_SKIPPED_RATE_LIMITED'
                                                            WHEN 'SupersededByNewerTick' THEN 'TICK_SKIPPED_SUPERSEDED'
                                                            ELSE 'TICK_SKIPPED_OTHER' END
                                    ELSE failure_type END AS kind,
                               COUNT(*) AS cnt
                        FROM quote_failure
                        WHERE received_at >= :since AND received_at <= :asOf
                        GROUP BY 1
                        ORDER BY 1
                        """)
                .param("since", since)
                .param("asOf", asOf)
                .query((rs, rowNum) -> counts.put(rs.getString("kind"), rs.getInt("cnt")))
                .list();
        return counts;
    }

    /** 처리하지 못하고 버린 메시지(failed_event)의 토픽별 건수. 버린 시각이 범위 안인 것만 센다. */
    List<DroppedRow> droppedMessages(OffsetDateTime since, OffsetDateTime asOf) {
        return jdbc.sql("""
                        SELECT topic, COUNT(*) AS cnt FROM failed_event
                        WHERE failed_at >= :since AND failed_at <= :asOf
                        GROUP BY topic
                        ORDER BY topic
                        """)
                .param("since", since)
                .param("asOf", asOf)
                .query((rs, rowNum) -> new DroppedRow(rs.getString("topic"), rs.getLong("cnt")))
                .list();
    }

    record CollectionRow(String kind, OffsetDateTime firstAt, OffsetDateTime lastAt, long count) {
    }

    record DroppedRow(String topic, long count) {
    }
}

package dev.stockreading.market.history;

import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** price_snapshot을 읽기만 한다. 응답을 만들기 위한 조회라 쓰기 경로는 없다. */
@Repository
class PriceHistoryRepository {

    private final JdbcClient jdbc;

    PriceHistoryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * [since, until] 구간(양끝 포함)의 스냅샷을 시간 순으로 최대 limit건 돌려준다. 계약이 until을 포함하므로
     * 끝을 제외하는 snapshot 쪽 조회를 재사용하지 않는다. 날짜 파티션 가지치기를 위해 항상 as_of 상·하한을 쓴다.
     */
    List<StoredPoint> findBetweenInclusive(String symbol, OffsetDateTime since, OffsetDateTime until, int limit) {
        return jdbc.sql("""
                        SELECT as_of, price FROM price_snapshot
                        WHERE symbol = :symbol AND as_of >= :since AND as_of <= :until
                        ORDER BY as_of
                        LIMIT :limit
                        """)
                .param("symbol", symbol)
                .param("since", since)
                .param("until", until)
                .param("limit", limit)
                .query((rs, rowNum) -> new StoredPoint(rs.getObject("as_of", OffsetDateTime.class), rs.getLong("price")))
                .list();
    }

    /**
     * [since, until] 구간(양끝 포함)을 since부터 intervalSeconds초 칸으로 나눠 칸마다 가장 이른 스냅샷 1건만 남기고,
     * 시간 순으로 최대 limit건 돌려준다. 칸의 첫 점을 남겨 기준이 되는 구간 앞쪽 가격이 빠지지 않게 한다.
     */
    List<StoredPoint> findThinnedBetweenInclusive(
            String symbol, OffsetDateTime since, OffsetDateTime until, int intervalSeconds, int limit) {
        return jdbc.sql("""
                        SELECT as_of, price FROM (
                            SELECT DISTINCT ON (bucket) as_of, price FROM (
                                SELECT as_of, price,
                                       floor(extract(epoch FROM (as_of - CAST(:since AS timestamptz))) / :interval) AS bucket
                                FROM price_snapshot
                                WHERE symbol = :symbol AND as_of >= :since AND as_of <= :until
                            ) AS rows_in_range
                            ORDER BY bucket, as_of
                        ) AS thinned
                        ORDER BY as_of
                        LIMIT :limit
                        """)
                .param("symbol", symbol)
                .param("since", since)
                .param("until", until)
                .param("interval", intervalSeconds)
                .param("limit", limit)
                .query((rs, rowNum) -> new StoredPoint(rs.getObject("as_of", OffsetDateTime.class), rs.getLong("price")))
                .list();
    }

    record StoredPoint(OffsetDateTime asOf, long price) {
    }
}

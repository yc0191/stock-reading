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

    record StoredPoint(OffsetDateTime asOf, long price) {
    }
}

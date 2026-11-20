package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** price_snapshot을 읽기만 한다. 날짜 파티션 가지치기를 위해 항상 as_of 상·하한을 쓴다. */
@Repository
class IntradaySeriesRepository implements IntradaySeriesPort {

    private final JdbcClient jdbc;

    IntradaySeriesRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<Point> minuteLasts(String symbol, OffsetDateTime since, OffsetDateTime asOf, int limit) {
        // 한국 시간은 UTC와 정시 단위로 어긋나므로 세션 시간대와 상관없이 같은 분으로 묶인다.
        // 같은 분 안에서는 시세 시각이 가장 늦은 것, 시세 시각이 같으면 가장 늦게 받은 것을 쓴다.
        return jdbc.sql("""
                        SELECT as_of, price FROM (
                            SELECT DISTINCT ON (date_trunc('minute', as_of)) as_of, price
                            FROM price_snapshot
                            WHERE symbol = :symbol AND as_of >= :since AND as_of <= :asOf AND received_at <= :asOf
                              AND price > 0
                            ORDER BY date_trunc('minute', as_of), as_of DESC, received_at DESC
                        ) minute_last
                        ORDER BY as_of DESC
                        LIMIT :limit
                        """)
                .param("symbol", symbol)
                .param("since", since)
                .param("asOf", asOf)
                .param("limit", limit)
                .query((rs, rowNum) -> new Point(rs.getObject("as_of", OffsetDateTime.class), rs.getLong("price")))
                .list();
    }
}

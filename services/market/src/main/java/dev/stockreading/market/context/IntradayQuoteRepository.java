package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** price_snapshot을 읽기만 한다. 날짜 파티션 가지치기를 위해 항상 as_of 상·하한을 쓴다. */
@Repository
class IntradayQuoteRepository implements IntradayQuotePort {

    private final JdbcClient jdbc;

    IntradayQuoteRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<Quote> lastReceived(String symbol, OffsetDateTime since, OffsetDateTime asOf) {
        return jdbc.sql("""
                        SELECT received_at, price, CAST(raw AS text) AS raw FROM price_snapshot
                        WHERE symbol = :symbol AND as_of >= :since AND as_of <= :asOf
                          AND received_at >= :since AND received_at <= :asOf
                        ORDER BY received_at DESC, as_of DESC
                        LIMIT 1
                        """)
                .param("symbol", symbol)
                .param("since", since)
                .param("asOf", asOf)
                .query((rs, rowNum) -> new Quote(rs.getObject("received_at", OffsetDateTime.class), rs.getLong("price"),
                        rs.getString("raw")))
                .optional();
    }
}

package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** price_snapshot을 읽기만 한다. 날짜 파티션 가지치기를 위해 항상 as_of 상·하한을 쓴다. */
@Repository
class OpenFlowRepository implements OpenFlowPort {

    private final JdbcClient jdbc;

    OpenFlowRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<Snapshot> first(String symbol, OffsetDateTime since, OffsetDateTime asOf) {
        return find(symbol, since, asOf, "ASC");
    }

    @Override
    public Optional<Snapshot> last(String symbol, OffsetDateTime since, OffsetDateTime asOf) {
        return find(symbol, since, asOf, "DESC");
    }

    private Optional<Snapshot> find(String symbol, OffsetDateTime since, OffsetDateTime asOf, String order) {
        // order는 이 클래스 안의 상수만 들어오므로 문자열로 이어 붙여도 주입 위험이 없다.
        return jdbc.sql("""
                        SELECT as_of, price, CAST(raw AS text) AS raw FROM price_snapshot
                        WHERE symbol = :symbol AND as_of >= :since AND as_of <= :asOf AND received_at <= :asOf
                        ORDER BY as_of %s
                        LIMIT 1
                        """.formatted(order))
                .param("symbol", symbol)
                .param("since", since)
                .param("asOf", asOf)
                .query((rs, rowNum) -> new Snapshot(rs.getObject("as_of", OffsetDateTime.class), rs.getLong("price"),
                        rs.getString("raw")))
                .optional();
    }
}

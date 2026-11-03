package dev.stockreading.market.insight;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class InsightRepository {

    private final JdbcClient jdbc;

    InsightRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 멱등 삽입: 다시 전달된 이벤트(같은 eventId)는 무시한다.
     * response는 해석하지 않고 jsonb로 그대로 저장한다(jsonb가 키 순서와 공백은 정규화한다).
     */
    boolean insertIfAbsent(ApiType type, UUID eventId, String target, String response, OffsetDateTime fetchedAt) {
        String columns = type.hasTarget() ? "event_id, target, response, fetched_at" : "event_id, response, fetched_at";
        String values = type.hasTarget()
                ? ":eventId, :target, CAST(:response AS jsonb), :fetchedAt"
                : ":eventId, CAST(:response AS jsonb), :fetchedAt";
        var statement = jdbc.sql("INSERT INTO " + type.table() + " (" + columns + ") VALUES (" + values + ") "
                        + "ON CONFLICT (event_id) DO NOTHING")
                .param("eventId", eventId)
                .param("response", response)
                .param("fetchedAt", fetchedAt);
        if (type.hasTarget()) {
            statement = statement.param("target", target);
        }
        return statement.update() == 1;
    }

    /**
     * 받은 시각이 fetchedUntil 이하인 해외 일봉 원문을 받은 시각이 늦은 순으로 limit 건 읽는다.
     * 원문은 jsonb에서 텍스트로 꺼낸 그대로다.
     */
    List<OverseasDailyRow> recentOverseasDailyPrices(String target, OffsetDateTime fetchedUntil, int limit) {
        return jdbc.sql("""
                        SELECT CAST(response AS text) AS response, fetched_at FROM overseas_daily_price
                        WHERE target = :target AND fetched_at <= :fetchedUntil
                        ORDER BY fetched_at DESC, id DESC
                        LIMIT :limit
                        """)
                .param("target", target)
                .param("fetchedUntil", fetchedUntil)
                .param("limit", limit)
                .query((rs, rowNum) -> new OverseasDailyRow(
                        rs.getString("response"), rs.getObject("fetched_at", OffsetDateTime.class)))
                .list();
    }
}

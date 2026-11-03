package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** market.insight가 저장한 배경 자료 원문을 읽기만 한다. 응답을 만들기 위한 조회라 쓰기 경로는 없다. */
@Repository
class BackgroundRepository {

    private final JdbcClient jdbc;

    BackgroundRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 받은 시각이 [since, asOf] 안(양끝 포함)인 원문 중 가장 늦게 받은 1건. target이 없는 종류는 target을 무시한다.
     * 원문은 jsonb에서 텍스트로 꺼낸 그대로다.
     */
    Optional<StoredResponse> latest(BackgroundKind kind, String target, OffsetDateTime since, OffsetDateTime asOf) {
        return select(kind, target, since, asOf, 1).stream().findFirst();
    }

    /** 세계지수 한 대륙의 원문 중 받은 시각이 [since, asOf] 안인 것을 받은 시각이 늦은 순으로 모두 읽는다. */
    List<StoredResponse> worldIndexRows(String continent, OffsetDateTime since, OffsetDateTime asOf) {
        return select(BackgroundKind.WORLD_INDEX, continent, since, asOf, null);
    }

    private List<StoredResponse> select(
            BackgroundKind kind, String target, OffsetDateTime since, OffsetDateTime asOf, Integer limit) {
        String sql = "SELECT CAST(response AS text) AS response, fetched_at FROM " + kind.table()
                + " WHERE fetched_at >= :since AND fetched_at <= :asOf"
                + (kind.hasTarget() ? " AND target = :target" : "")
                + " ORDER BY fetched_at DESC, id DESC"
                + (limit == null ? "" : " LIMIT :limit");
        var statement = jdbc.sql(sql).param("since", since).param("asOf", asOf);
        if (kind.hasTarget()) {
            statement = statement.param("target", target);
        }
        if (limit != null) {
            statement = statement.param("limit", limit);
        }
        return statement
                .query((rs, rowNum) -> new StoredResponse(
                        rs.getString("response"), rs.getObject("fetched_at", OffsetDateTime.class)))
                .list();
    }

    record StoredResponse(String response, OffsetDateTime fetchedAt) {
    }
}

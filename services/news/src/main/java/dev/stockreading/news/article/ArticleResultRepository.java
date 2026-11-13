package dev.stockreading.news.article;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class ArticleResultRepository {

    private final JdbcClient jdbc;

    ArticleResultRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 멱등 기록: 같은 이벤트는 한 행이고 처음 결과를 지킨다. 실패로 남은 이벤트를 나중에 다시 넣어 저장했으면
     * 그 결과로 바꾼다.
     */
    void record(UUID eventId, ArticleResult result, String correlationId) {
        jdbc.sql("""
                        INSERT INTO article_event_result (event_id, result, correlation_id)
                        VALUES (:eventId, :result, :correlationId)
                        ON CONFLICT (event_id) DO UPDATE
                            SET result = EXCLUDED.result, correlation_id = EXCLUDED.correlation_id, processed_at = now()
                            WHERE article_event_result.result = 'failed'
                        """)
                .param("eventId", eventId)
                .param("result", result.value())
                .param("correlationId", correlationId)
                .update();
    }

    /** 주어진 이벤트 중 처리가 끝난 것의 결과. 아직 처리되지 않은 이벤트는 결과에 없다. */
    Map<UUID, ArticleResult> resultsOf(Collection<UUID> eventIds) {
        Map<UUID, ArticleResult> results = new HashMap<>();
        if (eventIds.isEmpty()) {
            return results;
        }
        jdbc.sql("SELECT event_id, result FROM article_event_result WHERE event_id = ANY(CAST(:eventIds AS uuid[]))")
                .param("eventIds", eventIds.stream().map(UUID::toString).toArray(String[]::new))
                .query((rs, rowNum) -> results.put(rs.getObject("event_id", UUID.class),
                        ArticleResult.of(rs.getString("result"))))
                .list();
        return results;
    }
}

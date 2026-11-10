package dev.stockreading.news.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** news_article을 읽기만 한다. 응답을 만들기 위한 조회라 쓰기 경로는 없다. */
@Repository
class ArticleQueryRepository {

    private final JdbcClient jdbc;

    ArticleQueryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 해당 종목 또는 시장 전체("*") 태그가 붙은 기사를 먼저, 그다음 members(그 종목이 ETF일 때 담은 구성 종목)에 태그된
     * 기사(앞의 기사와 겹치는 기사 제외)를 [since, until] 구간(양끝 포함)에서 각각 최신순으로 이어 붙여 최대 limit건 돌려준다.
     * 구성 종목 기사가 아무리 많아도 종목 자체 기사가 먼저 남는다. 한 기사가 여러 태그를 가져도 한 건으로 나오도록
     * 조인 대신 EXISTS를 쓴다. 발행 시각이 같으면 id로 순서를 고정해 같은 조건의 응답이 늘 같은 순서가 되게 한다.
     */
    List<StoredArticle> findByRange(String symbol, Collection<String> members, OffsetDateTime since,
            OffsetDateTime until, int limit) {
        return jdbc.sql("""
                        SELECT a.article_key, a.title, a.summary, a.publisher, a.url, a.published_at
                        FROM news_article a
                        CROSS JOIN LATERAL (
                            SELECT EXISTS (
                                SELECT 1 FROM news_article_symbol s
                                WHERE s.article_id = a.id AND s.symbol IN (:symbol, '*')) AS own) o
                        WHERE a.published_at >= :since AND a.published_at <= :until
                          AND (o.own OR EXISTS (
                              SELECT 1 FROM news_article_symbol s
                              WHERE s.article_id = a.id AND s.symbol = ANY(CAST(:members AS varchar[]))))
                        ORDER BY o.own DESC, a.published_at DESC, a.id DESC
                        LIMIT :limit
                        """)
                .param("symbol", symbol)
                .param("members", members.toArray(String[]::new))
                .param("since", since)
                .param("until", until)
                .param("limit", limit)
                .query(ArticleQueryRepository::toStoredArticle)
                .list();
    }

    /**
     * 그 재검색 작업이 처음 찾은 기사(research_job_id 가 그 값인 기사)를 [since, until] 구간(양끝 포함)에서 최신순으로
     * 최대 limit건 돌려준다. 종목 태그는 보지 않는다. 순서 규칙(최신순, 같으면 id)은 findByRange 와 같다.
     */
    List<StoredArticle> findByResearchJob(UUID researchJobId, OffsetDateTime since, OffsetDateTime until, int limit) {
        return jdbc.sql("""
                        SELECT a.article_key, a.title, a.summary, a.publisher, a.url, a.published_at
                        FROM news_article a
                        WHERE a.research_job_id = :researchJobId
                          AND a.published_at >= :since AND a.published_at <= :until
                        ORDER BY a.published_at DESC, a.id DESC
                        LIMIT :limit
                        """)
                .param("researchJobId", researchJobId)
                .param("since", since)
                .param("until", until)
                .param("limit", limit)
                .query(ArticleQueryRepository::toStoredArticle)
                .list();
    }

    private static StoredArticle toStoredArticle(ResultSet rs, int rowNum) throws SQLException {
        return new StoredArticle(
                rs.getString("article_key"),
                rs.getString("title"),
                rs.getString("summary"),
                rs.getString("publisher"),
                rs.getString("url"),
                rs.getObject("published_at", OffsetDateTime.class));
    }

    record StoredArticle(
            String articleKey,
            String title,
            String summary,
            String publisher,
            String url,
            OffsetDateTime publishedAt) {
    }
}

package dev.stockreading.news.query;

import java.time.OffsetDateTime;
import java.util.List;

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
     * 해당 종목 또는 시장 전체("*") 태그가 붙은 기사를 [since, until] 구간(양끝 포함)에서 최신순으로
     * 최대 limit건 돌려준다. 한 기사가 두 태그를 모두 가져도 한 건으로 나오도록 조인 대신 EXISTS를 쓴다.
     * 발행 시각이 같으면 id로 순서를 고정해 같은 조건의 응답이 늘 같은 순서가 되게 한다.
     */
    List<StoredArticle> findByRange(String symbol, OffsetDateTime since, OffsetDateTime until, int limit) {
        return jdbc.sql("""
                        SELECT a.article_key, a.title, a.summary, a.publisher, a.url, a.published_at
                        FROM news_article a
                        WHERE a.published_at >= :since AND a.published_at <= :until
                          AND EXISTS (
                              SELECT 1 FROM news_article_symbol s
                              WHERE s.article_id = a.id AND s.symbol IN (:symbol, '*'))
                        ORDER BY a.published_at DESC, a.id DESC
                        LIMIT :limit
                        """)
                .param("symbol", symbol)
                .param("since", since)
                .param("until", until)
                .param("limit", limit)
                .query((rs, rowNum) -> new StoredArticle(
                        rs.getString("article_key"),
                        rs.getString("title"),
                        rs.getString("summary"),
                        rs.getString("publisher"),
                        rs.getString("url"),
                        rs.getObject("published_at", OffsetDateTime.class)))
                .list();
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

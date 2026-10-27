package dev.stockreading.news.article;

import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
class NewsArticleRepository {

    private final JdbcClient jdbc;

    NewsArticleRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 멱등 저장: 같은 이벤트를 몇 번 받아도, 같은 기사가 다른 이벤트로 와도 기사는 한 행이다.
     * 기존 기사면 종목과 검색어를 합집합으로 늘린다. 한 트랜잭션이라 중간 상태가 남지 않는다.
     *
     * @return 새 행이 만들어졌으면 true
     */
    @Transactional
    boolean save(NewsArticle article) {
        String articleKey = ArticleKey.of(article.url());
        String[] matchedQueries = article.matchedQueries().toArray(String[]::new);

        Optional<Long> inserted = insertIfAbsent(article, articleKey, matchedQueries);
        long articleId = inserted.orElseGet(() -> findIdByKey(articleKey));

        jdbc.sql("""
                        INSERT INTO news_article_symbol (article_id, symbol)
                        SELECT :articleId, s FROM unnest(CAST(:symbols AS varchar[])) AS s
                        ON CONFLICT DO NOTHING
                        """)
                .param("articleId", articleId)
                .param("symbols", article.symbols().toArray(String[]::new))
                .update();

        if (inserted.isEmpty()) {
            mergeMatchedQueries(articleId, matchedQueries);
        }
        return inserted.isPresent();
    }

    // 유니크 제약이 event_id와 article_key 둘이라 충돌 대상을 지정하지 않는다.
    private Optional<Long> insertIfAbsent(NewsArticle article, String articleKey, String[] matchedQueries) {
        return jdbc.sql("""
                        INSERT INTO news_article
                            (event_id, article_key, title, summary, url, publisher, published_at, matched_queries)
                        VALUES
                            (:eventId, :articleKey, :title, :summary, :url, :publisher, :publishedAt,
                             CAST(:matchedQueries AS text[]))
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """)
                .param("eventId", article.eventId())
                .param("articleKey", articleKey)
                .param("title", article.title())
                .param("summary", article.summary())
                .param("url", article.url())
                .param("publisher", article.publisher())
                .param("publishedAt", article.publishedAt())
                .param("matchedQueries", matchedQueries)
                .query(Long.class)
                .optional();
    }

    private long findIdByKey(String articleKey) {
        return jdbc.sql("SELECT id FROM news_article WHERE article_key = :articleKey")
                .param("articleKey", articleKey)
                .query(Long.class)
                .optional()
                .orElseThrow(() -> new IllegalStateException(
                        "article conflicted on event_id but not article_key, key=" + articleKey));
    }

    private void mergeMatchedQueries(long articleId, String[] matchedQueries) {
        jdbc.sql("""
                        UPDATE news_article
                        SET matched_queries = ARRAY(
                            SELECT DISTINCT q
                            FROM unnest(matched_queries || CAST(:matchedQueries AS text[])) AS q
                            ORDER BY q)
                        WHERE id = :articleId
                        """)
                .param("articleId", articleId)
                .param("matchedQueries", matchedQueries)
                .update();
    }
}

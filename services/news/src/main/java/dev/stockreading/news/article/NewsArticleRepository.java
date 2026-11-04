package dev.stockreading.news.article;

import java.util.Optional;
import java.util.UUID;

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
     * 재검색에서 온 기사면 재검색 여부를 참으로 두고, 작업 id 는 비어 있을 때만 채운다(처음 찾은 작업만 남긴다).
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
            if (article.researchJobId() != null) {
                markFoundByResearch(articleId, article.researchJobId());
            }
        }
        return inserted.isPresent();
    }

    // 유니크 제약이 event_id와 article_key 둘이라 충돌 대상을 지정하지 않는다.
    private Optional<Long> insertIfAbsent(NewsArticle article, String articleKey, String[] matchedQueries) {
        return jdbc.sql("""
                        INSERT INTO news_article
                            (event_id, article_key, title, summary, url, publisher, published_at, matched_queries,
                             from_research, research_job_id)
                        VALUES
                            (:eventId, :articleKey, :title, :summary, :url, :publisher, :publishedAt,
                             CAST(:matchedQueries AS text[]), :fromResearch, :researchJobId)
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
                .param("fromResearch", article.researchJobId() != null)
                .param("researchJobId", article.researchJobId())
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

    // 정기 수집으로 먼저 저장된 기사도 재검색이 찾았으면 재검색 기사로 표시한다. 이미 다른 작업 id 가 있으면
    // 덮어쓰지 않으므로 두 번째 작업과의 연결은 남지 않는다(한 기사에 작업 id 하나만 두기로 한 결정).
    private void markFoundByResearch(long articleId, UUID researchJobId) {
        jdbc.sql("""
                        UPDATE news_article
                        SET from_research = true,
                            research_job_id = COALESCE(research_job_id, :researchJobId)
                        WHERE id = :articleId
                        """)
                .param("articleId", articleId)
                .param("researchJobId", researchJobId)
                .update();
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

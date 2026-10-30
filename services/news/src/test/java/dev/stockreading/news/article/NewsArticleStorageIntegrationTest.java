package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import tools.jackson.databind.json.JsonMapper;

/**
 * 기사 저장을 실제 Postgres에서 저장 후 다시 읽어 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 news 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
class NewsArticleStorageIntegrationTest {

    private static JdbcClient jdbc;
    private static NewsListener listener;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("NEWS_IT_DB_URL");
        String user = System.getenv("NEWS_IT_DB_USER");
        String password = System.getenv("NEWS_IT_DB_PASSWORD");

        // 서비스 설정(application.yml)과 같이 news 스키마에만 마이그레이션하고, 스키마는 만들지 않는다.
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("news").defaultSchema("news").createSchemas(false).load().migrate();

        jdbc = JdbcClient.create(new DriverManagerDataSource(url + (url.contains("?") ? "&" : "?") + "currentSchema=news",
                user, password));
        listener = new NewsListener(new NewsArticleRepository(jdbc), JsonMapper.builder().build());
    }

    @Test
    void sameEventReceivedTwiceLeavesOneArticle() {
        String message = event("d1000000-0000-4000-8000-000000000001", "https://news.example.com/it/1001",
                "[\"ITN001\"]", "[\"검색어A\"]");

        listener.onNews(message);
        listener.onNews(message);

        assertThat(articles("https://news.example.com/it/1001")).isEqualTo(1);
        assertThat(symbolsOf("news.example.com/it/1001")).isEqualTo("ITN001");
    }

    @Test
    void sameArticleKeyFromAnotherEventKeepsOneRowAndMergesSymbolsAndQueries() {
        listener.onNews(event("d2000000-0000-4000-8000-000000000001", "https://www.news.example.com/it/1002/?utm_source=a",
                "[\"ITN002\"]", "[\"검색어B\"]"));
        // 추적 파라미터와 www, 끝의 / 만 다른 같은 기사다.
        listener.onNews(event("d2000000-0000-4000-8000-000000000002", "https://news.example.com/it/1002",
                "[\"ITN002\",\"*\"]", "[\"검색어A\"]"));

        Map<String, Object> row = jdbc.sql("""
                        SELECT count(*) AS n, min(url) AS url, min(array_to_string(matched_queries, ',')) AS queries
                        FROM news_article WHERE url LIKE '%/it/1002%'
                        """).query().singleRow();
        // 첫 기사의 원래 URL은 그대로 두고, 종목과 검색어만 합친다.
        assertThat(row).containsEntry("n", 1L)
                .containsEntry("url", "https://www.news.example.com/it/1002/?utm_source=a")
                .containsEntry("queries", "검색어A,검색어B");
        assertThat(symbolsOf("1002")).isEqualTo("*,ITN002");
    }

    private static long articles(String url) {
        return jdbc.sql("SELECT count(*) FROM news_article WHERE url = :url").param("url", url).query(Long.class).single();
    }

    private static String symbolsOf(String urlPart) {
        return jdbc.sql("""
                        SELECT string_agg(s.symbol, ',' ORDER BY s.symbol COLLATE "C") FROM news_article_symbol s
                        JOIN news_article a ON a.id = s.article_id WHERE a.url LIKE :pattern
                        """).param("pattern", "%" + urlPart + "%").query(String.class).single();
    }

    private static String event(String eventId, String url, String symbolsJson, String queriesJson) {
        return """
                {"eventId":"%s","eventType":"news.collected","schemaVersion":1,
                 "occurredAt":"2026-03-10T09:00:00+09:00","producedAt":"2026-03-10T09:00:01+09:00","correlationId":"it",
                 "payload":{"title":"시험 기사","summary":"시험 요약","url":"%s","publisher":"news.example.com",
                            "publishedAt":"2026-03-10T09:00:00+09:00","symbols":%s,"matchedQueries":%s}}
                """.formatted(eventId, url, symbolsJson, queriesJson);
    }
}

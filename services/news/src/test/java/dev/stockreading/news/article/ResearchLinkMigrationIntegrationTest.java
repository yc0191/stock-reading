package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 기사마다 하나만 남던 재검색 작업 id(news_article.research_job_id)가 기사-작업 연결 표로 옮겨지는지 실제 Postgres로 확인한다.
 * 운영 표(news)를 건드리지 않도록 환경변수(NEWS_IT_DB_MIGRATION_SCHEMA)가 가리키는 별도 스키마에서만 돌며,
 * 그 스키마는 시작할 때 비우고(clean) 옛 버전(V8)까지 적용한 뒤 옛 행을 넣고 최신으로 올린다.
 */
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_MIGRATION_SCHEMA", matches = "[a-z_]+")
class ResearchLinkMigrationIntegrationTest {

    private static final String DISPOSABLE_PREFIX = "stock_test_run_";
    private static final String JOB = "e1000000-0000-4000-8000-000000000001";

    private static Flyway flyway(String schema, String target) {
        return Flyway.configure()
                .dataSource(System.getenv("NEWS_IT_DB_URL"), System.getenv("NEWS_IT_DB_USER"),
                        System.getenv("NEWS_IT_DB_PASSWORD"))
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration")
                .cleanDisabled(false).target(target).load();
    }

    @Test
    void theFirstResearchJobOfOldArticlesMovesToTheLinkTableAndRegularArticlesGetNoLink() {
        String schema = System.getenv("NEWS_IT_DB_MIGRATION_SCHEMA");
        // 운영 표를 비우지 않도록 시작 전에 막는다.
        assertThat(schema).isNotEqualTo("news").isNotEqualTo("public");
        JdbcClient jdbc = JdbcClient.create(new DriverManagerDataSource(System.getenv("NEWS_IT_DB_URL"),
                System.getenv("NEWS_IT_DB_USER"), System.getenv("NEWS_IT_DB_PASSWORD")));
        assertThat(jdbc.sql("SELECT current_database()").query(String.class).single()).startsWith(DISPOSABLE_PREFIX);
        flyway(schema, "8").clean();
        flyway(schema, "8").migrate();
        String s = schema + ".";
        jdbc.sql("INSERT INTO " + s + "news_article (id, event_id, article_key, title, summary, url, publisher, "
                + "published_at, received_at, from_research, research_job_id) VALUES "
                + "(1, 'e2000000-0000-4000-8000-000000000001', repeat('a', 64), '재검색 기사', '', "
                + "'https://news.example.com/m/1', 'news.example.com', TIMESTAMPTZ '2026-10-20 07:00+09', "
                + "TIMESTAMPTZ '2026-10-20 08:00+09', true, '" + JOB + "'), "
                + "(2, 'e2000000-0000-4000-8000-000000000002', repeat('b', 64), '정기 수집 기사', '', "
                + "'https://news.example.com/m/2', 'news.example.com', TIMESTAMPTZ '2026-10-20 07:00+09', "
                + "TIMESTAMPTZ '2026-10-20 08:00+09', false, NULL)").update();

        flyway(schema, "latest").migrate();

        // 옛 칸에 있던 처음 작업만 연결되고, 찾은 시각은 그 기사를 받은 시각이다.
        assertThat(jdbc.sql("SELECT article_id || ':' || research_job_id || ':' || (found_at = TIMESTAMPTZ "
                + "'2026-10-20 08:00+09') FROM " + s + "news_article_research ORDER BY article_id")
                .query(String.class).list()).containsExactly("1:" + JOB + ":true");
        // 옛 칸은 지우지 않는다.
        assertThat(jdbc.sql("SELECT research_job_id::text FROM " + s + "news_article WHERE id = 1")
                .query(String.class).single()).isEqualTo(JOB);
    }
}

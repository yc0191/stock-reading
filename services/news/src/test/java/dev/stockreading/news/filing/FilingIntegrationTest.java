package dev.stockreading.news.filing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import tools.jackson.databind.json.JsonMapper;

/**
 * 실제 Postgres 대상 통합 테스트. 세 환경변수가 모두 있을 때만 실행하며, 대상은 비어 있는 새 DB(public 스키마)라고 가정한다.
 * 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
class FilingIntegrationTest {

    private static JdbcClient jdbc;
    private static FilingListener listener;
    private static FilingQuery query;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("NEWS_IT_DB_URL");
        String user = System.getenv("NEWS_IT_DB_USER");
        String password = System.getenv("NEWS_IT_DB_PASSWORD");

        // 서비스와 같은 classpath 마이그레이션을 적용한다(스키마는 기본값인 public).
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();

        DriverManagerDataSource dataSource = new DriverManagerDataSource(url, user, password);
        jdbc = JdbcClient.create(dataSource);
        FilingRepository repository = new FilingRepository(jdbc);
        listener = new FilingListener(repository, JsonMapper.builder().build());
        query = new FilingQuery(repository);
    }

    @Test
    void migrationCreatesTheFilingTablesNextToTheNewsTables() {
        List<String> tables = jdbc.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                .query(String.class).list();

        assertThat(tables).contains("filing", "filing_ticker", "news_article", "news_article_symbol", "failed_event");
    }

    @Test
    void sameEventIdReceivedTwiceLeavesOneRow() {
        String message = event("11111111-1111-4111-8111-111111111111", "0000000011-26-000001", "[\"ITESTA\"]",
                "2026-10-20T20:30:28Z", "[\"2.02\",\"9.01\"]");

        listener.onFiling(message);
        listener.onFiling(message);

        assertThat(count("accession_number = '0000000011-26-000001'")).isEqualTo(1);
        assertThat(jdbc.sql("SELECT array_to_string(items, ',') FROM filing WHERE accession_number = '0000000011-26-000001'")
                .query(String.class).single()).isEqualTo("2.02,9.01");
    }

    @Test
    void sameAccessionFromAnotherEventKeepsOneFilingAndAddsTheNewTicker() {
        listener.onFiling(event("22222222-2222-4222-8222-222222222221", "0000000022-26-000001", "[\"ITESTB\"]",
                "2026-10-20T20:30:28Z", "[]"));
        listener.onFiling(event("22222222-2222-4222-8222-222222222222", "0000000022-26-000001", "[\"ITESTB\",\"ITESTB.A\"]",
                "2026-10-20T20:30:28Z", "[]"));

        assertThat(count("accession_number = '0000000022-26-000001'")).isEqualTo(1);
        assertThat(jdbc.sql("""
                        SELECT t.ticker FROM filing_ticker t JOIN filing f ON f.id = t.filing_id
                        WHERE f.accession_number = '0000000022-26-000001' ORDER BY t.ticker
                        """).query(String.class).list()).containsExactly("ITESTB", "ITESTB.A");
    }

    @Test
    void queryReturnsTheTickersFilingsInThePeriodUpToAndIncludingTheReferenceTime() {
        OffsetDateTime asOf = OffsetDateTime.parse("2026-10-22T14:00:00Z");
        listener.onFiling(event("33333333-3333-4333-8333-333333333331", "0000000033-26-000001", "[\"ITESTC\"]",
                "2026-10-21T13:59:59Z", "[\"8.01\"]"));
        listener.onFiling(event("33333333-3333-4333-8333-333333333332", "0000000033-26-000002", "[\"ITESTC\"]",
                "2026-10-22T13:59:59Z", "[\"2.02\"]"));
        listener.onFiling(event("33333333-3333-4333-8333-333333333333", "0000000033-26-000003", "[\"ITESTC\"]",
                "2026-10-22T14:00:00Z", "[\"7.01\"]"));
        listener.onFiling(event("33333333-3333-4333-8333-333333333334", "0000000033-26-000004", "[\"ITESTC\"]",
                "2026-10-22T14:00:01Z", "[\"5.02\"]"));
        listener.onFiling(event("33333333-3333-4333-8333-333333333335", "0000000033-26-000005", "[\"ITESTD\"]",
                "2026-10-22T13:00:00Z", "[\"2.02\"]"));

        List<StoredFiling> found = query.find("ITESTC", asOf.minusHours(24), asOf.plusHours(1), asOf);

        // 기간 시작 전(1초 이른 것), 기준 시각 뒤, 다른 종목의 공시는 빠진다.
        assertThat(found).extracting(StoredFiling::accessionNumber)
                .containsExactly("0000000033-26-000002", "0000000033-26-000003");
        assertThat(found.get(0).items()).containsExactly("2.02");
        assertThat(found.get(0).tickers()).containsExactly("ITESTC");
        assertThat(found.get(1).acceptedAt().toInstant()).isEqualTo(asOf.toInstant());
    }

    private static long count(String where) {
        return jdbc.sql("SELECT count(*) FROM filing WHERE " + where).query(Long.class).single();
    }

    private static String event(String eventId, String accession, String tickersJson, String acceptedAt, String itemsJson) {
        return """
                {"eventId":"%s","eventType":"filing.collected","schemaVersion":1,
                 "occurredAt":"%s","producedAt":"2026-10-22T15:00:00Z","correlationId":"it",
                 "payload":{"tickers":%s,"cik":"0000000011","accessionNumber":"%s","form":"8-K","items":%s,
                            "acceptedAt":"%s","filingDate":"2026-10-20",
                            "url":"https://www.sec.gov/Archives/edgar/data/11/x/test.htm"}}
                """.formatted(eventId, acceptedAt, tickersJson, accession, itemsJson, acceptedAt);
    }
}

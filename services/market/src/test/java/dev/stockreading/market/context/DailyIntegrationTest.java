package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 데일리 판 자료 조회의 실제 Postgres 통합 테스트. 세 환경변수가 모두 있을 때만 실행하며, 대상은 비어 있는 새 DB(public 스키마)라고
 * 가정한다. 보유 ETF 조회는 시각대 안의 모든 종목을 읽으므로 테스트마다 다른 시각대(다른 테스트 클래스와도 겹치지 않는 해)를 쓴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class DailyIntegrationTest {

    private static JdbcClient jdbc;
    private static BackgroundRepository repository;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url, user, password));
        repository = new BackgroundRepository(jdbc);
    }

    @Test
    void eachSymbolGivesItsLatestRowAtOrBeforeTheReferenceTimeInSymbolOrder() {
        OffsetDateTime asOf = OffsetDateTime.parse("2025-05-10T08:00:00Z");
        insert("DT0002", "{\"v\": \"b-at\"}", asOf);
        insert("DT0002", "{\"v\": \"b-after\"}", asOf.plusSeconds(1));
        insert("DT0002", "{\"v\": \"b-older\"}", asOf.minusHours(1));
        insert("DT0001", "{\"v\": \"a\"}", asOf.minusHours(2));

        List<BackgroundRepository.StoredTargetResponse> rows = repository.latestDailyPrices(asOf.minusDays(4), asOf);

        assertThat(rows).extracting(BackgroundRepository.StoredTargetResponse::target)
                .containsExactly("DT0001", "DT0002");
        assertThat(rows.get(1).row().response()).isEqualTo("{\"v\": \"b-at\"}");
        assertThat(rows.get(1).row().fetchedAt()).isEqualTo(asOf);
    }

    @Test
    void symbolWhoseOnlyRowsAreOutsideTheRangeIsNotAHolding() {
        OffsetDateTime asOf = OffsetDateTime.parse("2025-05-20T08:00:00Z");
        OffsetDateTime since = asOf.minusDays(4);
        insert("DT0003", "{\"v\": 1}", since.minusSeconds(1));
        insert("DT0004", "{\"v\": 2}", asOf.plusSeconds(1));
        insert("DT0005", "{\"v\": 3}", since);

        assertThat(repository.latestDailyPrices(since, asOf))
                .extracting(BackgroundRepository.StoredTargetResponse::target)
                .containsExactly("DT0005");
    }

    @Test
    void queryOnlyReadsAndLeavesTheRowsAsTheyWere() {
        OffsetDateTime asOf = OffsetDateTime.parse("2025-05-30T08:00:00Z");
        insert("DT0006", "{\"v\": 1}", asOf);
        long before = count();

        repository.latestDailyPrices(asOf.minusDays(4), asOf);
        repository.latestDailyPrices(asOf.minusDays(4), asOf);

        assertThat(count()).isEqualTo(before);
    }

    private static long count() {
        return jdbc.sql("SELECT count(*) FROM daily_price").query(Long.class).single();
    }

    private static void insert(String target, String response, OffsetDateTime fetchedAt) {
        jdbc.sql("INSERT INTO daily_price (event_id, target, response, fetched_at) "
                        + "VALUES (:eventId, :target, CAST(:response AS jsonb), :fetchedAt)")
                .param("eventId", UUID.randomUUID())
                .param("target", target)
                .param("response", response)
                .param("fetchedAt", fetchedAt)
                .update();
    }
}

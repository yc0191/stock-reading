package dev.stockreading.market.history;

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
 * 실제 Postgres 대상 통합 테스트. 세 환경변수가 모두 있을 때만 실행하며, 대상은 비어 있는 새 DB(public 스키마)라고 가정한다.
 * 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class PriceHistoryIntegrationTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-20T06:55:31Z");

    private static JdbcClient jdbc;
    private static PriceHistoryRepository repository;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url, user, password));
        repository = new PriceHistoryRepository(jdbc);
        // 서비스가 저장 직전에 만드는 규칙과 같은 이름·경계(KST 자정)의 파티션을 미리 만든다.
        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS price_snapshot_p20261020 PARTITION OF price_snapshot
                FOR VALUES FROM ('2026-10-20 00:00:00+09') TO ('2026-10-21 00:00:00+09')
                """).update();
        // 1초마다 한 점, since부터 35초 동안. 가격은 순번이라 어느 점이 남았는지 알 수 있다.
        for (int i = 0; i < 35; i++) {
            jdbc.sql("INSERT INTO price_snapshot (event_id, symbol, as_of, price) VALUES (:id, 'HT0001', :asOf, :price)")
                    .param("id", UUID.randomUUID()).param("asOf", SINCE.plusSeconds(i)).param("price", 10_000 + i)
                    .update();
        }
    }

    @Test
    void thinnedQueryKeepsTheFirstPointOfEveryIntervalFromSince() {
        List<PriceHistoryRepository.StoredPoint> points =
                repository.findThinnedBetweenInclusive("HT0001", SINCE, SINCE.plusSeconds(34), 10, 100);

        assertThat(points).extracting(PriceHistoryRepository.StoredPoint::price)
                .containsExactly(10_000L, 10_010L, 10_020L, 10_030L);
    }

    @Test
    void thinnedQueryStopsAtTheLimitFromTheFront() {
        List<PriceHistoryRepository.StoredPoint> points =
                repository.findThinnedBetweenInclusive("HT0001", SINCE, SINCE.plusSeconds(34), 10, 2);

        assertThat(points).extracting(PriceHistoryRepository.StoredPoint::price).containsExactly(10_000L, 10_010L);
    }

    @Test
    void unthinnedQueryStillReturnsEveryPointWithBothEndsIncluded() {
        List<PriceHistoryRepository.StoredPoint> points =
                repository.findBetweenInclusive("HT0001", SINCE.plusSeconds(5), SINCE.plusSeconds(9), 100);

        assertThat(points).extracting(PriceHistoryRepository.StoredPoint::price)
                .containsExactly(10_005L, 10_006L, 10_007L, 10_008L, 10_009L);
    }
}

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

import dev.stockreading.market.event.ContextPayload;
import tools.jackson.databind.json.JsonMapper;

/**
 * 실제 Postgres 대상 통합 테스트. 세 환경변수가 모두 있을 때만 실행하며, 대상은 비어 있는 새 DB(public 스키마)라고 가정한다.
 * 평소 gradlew test 에서는 건너뛴다. 테스트마다 다른 target과 시각대를 써서 서로의 행을 읽지 않게 한다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class ContextIntegrationTest {

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
    void latestRowFetchedAtTheReferenceTimeIsIncludedAndOneSecondLaterIsNot() {
        OffsetDateTime asOf = OffsetDateTime.parse("2026-09-10T07:00:00Z");
        insert("daily_price", "CT0001", "{\"v\": \"at\"}", asOf);
        insert("daily_price", "CT0001", "{\"v\": \"after\"}", asOf.plusSeconds(1));
        insert("daily_price", "CT0001", "{\"v\": \"older\"}", asOf.minusHours(1));

        BackgroundRepository.StoredResponse row = repository.latest(
                BackgroundKind.DAILY_PRICE, "CT0001", asOf.minusDays(4), asOf).orElseThrow();

        assertThat(row.response()).isEqualTo("{\"v\": \"at\"}");
        assertThat(row.fetchedAt()).isEqualTo(asOf);
    }

    @Test
    void rowOlderThanTheLowerBoundIsMissingButOneExactlyAtTheBoundIsKept() {
        OffsetDateTime asOf = OffsetDateTime.parse("2026-09-20T07:00:00Z");
        OffsetDateTime since = asOf.minusDays(4);
        insert("daily_price", "CT0002", "{\"v\": 1}", since.minusSeconds(1));
        insert("daily_price", "CT0003", "{\"v\": 2}", since);

        assertThat(repository.latest(BackgroundKind.DAILY_PRICE, "CT0002", since, asOf)).isEmpty();
        assertThat(repository.latest(BackgroundKind.DAILY_PRICE, "CT0003", since, asOf)).isPresent();
    }

    @Test
    void kindsWithoutTargetReadTheirOwnTable() {
        OffsetDateTime asOf = OffsetDateTime.parse("2026-08-10T07:00:00Z");
        insert("exchange_rate_summary", null, "{\"k\": \"fx\"}", asOf.minusMinutes(5));
        insert("market_summary", null, "{\"k\": \"mk\"}", asOf.minusMinutes(3));

        assertThat(repository.latest(BackgroundKind.EXCHANGE_RATE_SUMMARY, null, asOf.minusDays(4), asOf)
                .orElseThrow().response()).isEqualTo("{\"k\": \"fx\"}");
        assertThat(repository.latest(BackgroundKind.MARKET_SUMMARY, null, asOf.minusDays(4), asOf)
                .orElseThrow().response()).isEqualTo("{\"k\": \"mk\"}");
    }

    @Test
    void worldIndexRatesAreComputedFromStoredRowsOfTheSameContinent() {
        OffsetDateTime asOf = OffsetDateTime.parse("2026-07-13T00:00:00Z");
        OffsetDateTime since = asOf.minusDays(4);
        insert("world_index", "CTX", worldIndex("20261010", " 102.00"), asOf.minusHours(1));
        // 같은 지수 코드를 담은 다른 대륙 행과 기준 시각 뒤에 받은 행은 쓰지 않는다.
        insert("world_index", "CTY", worldIndex("20261009", "  50.00"), asOf.minusHours(2));
        insert("world_index", "CTX", worldIndex("20261009", "  90.00"), asOf.plusSeconds(1));
        insert("world_index", "CTX", worldIndex("20261009", " 100.00"), asOf.minusDays(1));

        List<BackgroundRepository.StoredResponse> rows = repository.worldIndexRows("CTX", since, asOf);
        List<ContextPayload.IndexRate> rates = new WorldIndexRates(JsonMapper.builder().build())
                .compute(rows.get(0).response(), rows, since, asOf);

        assertThat(rows).hasSize(2);
        assertThat(rates).containsExactly(new ContextPayload.IndexRate(
                "CTIDX", "CT INDEX", "20261010", "102.00", "20261009", "100.00", "2.00"));
    }

    private static String worldIndex(String day, String close) {
        return "{\"dataBody\": {\"out2\": [{\"is_cd\": \"CTIDX\", \"is_cd_nm\": \"CT INDEX\", \"bsnss_dy\": \"" + day
                + "\", \"cls_prc_p2\": \"" + close + "\", \"bdy_cmpr_r_p2\": \"0.00\"}]}}";
    }

    private static void insert(String table, String target, String response, OffsetDateTime fetchedAt) {
        String columns = target == null ? "event_id, response, fetched_at" : "event_id, target, response, fetched_at";
        String values = target == null
                ? ":eventId, CAST(:response AS jsonb), :fetchedAt"
                : ":eventId, :target, CAST(:response AS jsonb), :fetchedAt";
        var statement = jdbc.sql("INSERT INTO " + table + " (" + columns + ") VALUES (" + values + ")")
                .param("eventId", UUID.randomUUID())
                .param("response", response)
                .param("fetchedAt", fetchedAt);
        if (target != null) {
            statement = statement.param("target", target);
        }
        statement.update();
    }
}

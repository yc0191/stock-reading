package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.stockreading.market.event.DailyPayload;
import tools.jackson.databind.json.JsonMapper;

/**
 * 개장 흐름의 시세 조회와 계산을 실제 Postgres에 저장한 스냅샷으로 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 market 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class OpenFlowIntegrationTest {

    // 다른 통합 테스트(03-10 파티션, ITH 종목)와 날짜·종목이 겹치지 않게 03-12와 OPF 종목을 쓴다.
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-03-12T09:00:00+09:00");
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-03-12T09:10:00+09:00");
    private static final String RAW = "{\"dataBody\":{\"opn_prc\":\"10200\",\"bdy_cls_prc\":\"10000\","
            + "\"sprc\":\"10000\",\"krx_bdy_clpr\":\"10000\"}}";

    private static JdbcClient jdbc;
    private static OpenFlowRepository repository;

    @BeforeAll
    static void migrateAndSeed() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");

        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("market").defaultSchema("market").createSchemas(false).load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url + (url.contains("?") ? "&" : "?") + "currentSchema=market",
                user, password));
        repository = new OpenFlowRepository(jdbc);

        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS price_snapshot_p20260312 PARTITION OF price_snapshot
                FOR VALUES FROM ('2026-03-12T00:00:00+09:00') TO ('2026-03-13T00:00:00+09:00')
                """).update();
        insert("OPF001", SINCE.minusSeconds(1), 9_900, null, SINCE.minusSeconds(1));
        insert("OPF001", SINCE.plusSeconds(1), 10_100, null, SINCE.plusSeconds(1));
        insert("OPF001", AS_OF.minusSeconds(2), 10_300, RAW, AS_OF.minusSeconds(2));
        // 시세 시각은 범위 안이지만 판 시작 뒤에 받은 행: 쓰지 않아야 한다.
        insert("OPF001", AS_OF.minusSeconds(1), 10_900, RAW, AS_OF.plusSeconds(1));
        // 시세 시각이 판 시작 뒤인 행도 쓰지 않는다.
        insert("OPF001", AS_OF.plusSeconds(1), 11_000, RAW, AS_OF.plusSeconds(1));
        // 다른 종목의 행은 읽지 않아야 한다.
        insert("OPF002", SINCE.plusSeconds(5), 20_000, null, SINCE.plusSeconds(5));
    }

    @Test
    void readsTheFirstAndLastQuoteInsideTheRangeThatWereReceivedByAsOf() {
        Optional<OpenFlowPort.Snapshot> first = repository.first("OPF001", SINCE, AS_OF);
        Optional<OpenFlowPort.Snapshot> last = repository.last("OPF001", SINCE, AS_OF);

        assertThat(first).get().extracting(OpenFlowPort.Snapshot::price).isEqualTo(10_100L);
        assertThat(last).get().extracting(OpenFlowPort.Snapshot::price).isEqualTo(10_300L);
        assertThat(last.get().raw()).contains("\"opn_prc\"");
    }

    @Test
    void anotherSymbolAndAnEmptyRangeGiveNothing() {
        assertThat(repository.first("OPF003", SINCE, AS_OF)).isEmpty();
        assertThat(repository.last("OPF001", AS_OF.plusMinutes(1), AS_OF.plusMinutes(2))).isEmpty();
    }

    @Test
    void storedRowsGoThroughTheCalculatorToTheDocumentedRates() {
        OpenFlowCalculator calculator = new OpenFlowCalculator(repository, JsonMapper.builder().build(),
                List.of("dataBody.opn_prc"), List.of("dataBody.bdy_cls_prc", "dataBody.sprc", "dataBody.krx_bdy_clpr"));

        DailyPayload.OpenFlow flow = calculator.calculate("OPF001", SINCE, AS_OF);

        assertThat(flow.open()).isEqualTo("10200");
        assertThat(flow.openRate()).isEqualTo("2.00");
        assertThat(flow.endPrice()).isEqualTo("10300");
        assertThat(flow.rateVsPrevClose()).isEqualTo("3.00");
        assertThat(calculator.calculate("OPF003", SINCE, AS_OF).status()).isEqualTo("no-snapshot");
    }

    private static void insert(String symbol, OffsetDateTime asOf, long price, String raw, OffsetDateTime receivedAt) {
        jdbc.sql("""
                        INSERT INTO price_snapshot (event_id, symbol, as_of, price, raw, received_at)
                        VALUES (gen_random_uuid(), :symbol, :asOf, :price, CAST(CAST(:raw AS text) AS jsonb), :receivedAt)
                        """)
                .param("symbol", symbol).param("asOf", asOf).param("price", price).param("raw", raw)
                .param("receivedAt", receivedAt).update();
    }
}

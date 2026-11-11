package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import tools.jackson.databind.json.JsonMapper;

/**
 * 시세 저장을 실제 Postgres에서 저장 후 다시 읽어 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 market 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class SnapshotStorageIntegrationTest {

    private static JdbcClient jdbc;
    private static MoveTracker moveTracker;
    private static SnapshotListener listener;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");

        // 서비스 설정(application.yml)과 같이 market 스키마에만 마이그레이션하고, 스키마는 만들지 않는다.
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("market").defaultSchema("market").createSchemas(false).load().migrate();

        DriverManagerDataSource dataSource = new DriverManagerDataSource(withMarketSchema(url), user, password);
        jdbc = JdbcClient.create(dataSource);
        TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        moveTracker = mock(MoveTracker.class);
        listener = new SnapshotListener(new PriceSnapshotRepository(jdbc),
                new PricePartitions(new JdbcPartitionPort(jdbc, tx)), moveTracker, JsonMapper.builder().build());
    }

    @Test
    void storesASnapshotOnADayThatHasNoPartitionYet() {
        // 한국 시각 자정 정각은 그날 파티션에 들어가야 한다(UTC로는 전날 15시).
        assertThat(partitionExists("price_snapshot_p20260304")).isFalse();

        listener.onSnapshot(message("a1000000-0000-4000-8000-000000000001", "ITS001", 10_250,
                "2026-03-04T00:00:00+09:00", "{\"price\":\"10250\"}"));

        assertThat(partitionExists("price_snapshot_p20260304")).isTrue();
        Map<String, Object> row = jdbc.sql("""
                        SELECT tableoid::regclass::text AS partition, price, as_of = CAST('2026-03-03T15:00:00Z' AS timestamptz) AS same_instant
                        FROM price_snapshot WHERE symbol = 'ITS001'
                        """).query().singleRow();
        assertThat(row).containsEntry("partition", "price_snapshot_p20260304").containsEntry("price", 10_250L)
                .containsEntry("same_instant", true);
    }

    @Test
    void rawResponseIsStoredAsIsExceptForTrimming() {
        String raw = "  {\"output\": {\"close\": \" 10000 \", \"rate\": 1.10, \"big\": 12345678901234567890}, \"한글\": \"값\"}\n";

        listener.onSnapshot(message("a2000000-0000-4000-8000-000000000001", "ITS002", 10_000,
                "2026-03-05T10:00:00+09:00", raw));

        Map<String, Object> row = jdbc.sql("""
                        SELECT raw #>> '{output,close}' AS close, raw #>> '{output,rate}' AS rate,
                               raw #>> '{output,big}' AS big, raw ->> '한글' AS korean
                        FROM price_snapshot WHERE symbol = 'ITS002'
                        """).query().singleRow();
        // 문자열 안의 공백, 소수 자릿수, 큰 정수, 한글이 값 수준에서 그대로 남아야 한다.
        assertThat(row).containsEntry("close", " 10000 ").containsEntry("rate", "1.10")
                .containsEntry("big", "12345678901234567890").containsEntry("korean", "값");
    }

    @Test
    void rawResponseThatIsNotJsonIsKeptAsATrimmedString() {
        listener.onSnapshot(message("a3000000-0000-4000-8000-000000000001", "ITS003", 10_000,
                "2026-03-05T10:00:00+09:00", "  <html>error page</html>  "));

        assertThat(jdbc.sql("SELECT raw #>> '{}' FROM price_snapshot WHERE symbol = 'ITS003'").query(String.class).single())
                .isEqualTo("<html>error page</html>");
    }

    @Test
    void sameEventIdReceivedTwiceIsStoredOnceAndTrackedOnce() {
        String message = message("a4000000-0000-4000-8000-000000000001", "ITS004", 10_100,
                "2026-03-05T10:00:01+09:00", "{\"price\":\"10100\"}");

        listener.onSnapshot(message);
        listener.onSnapshot(message);

        assertThat(jdbc.sql("SELECT count(*) FROM price_snapshot WHERE symbol = 'ITS004'").query(Long.class).single())
                .isEqualTo(1);
        verify(moveTracker, times(1)).onSnapshot(eq("ITS004"), any(OffsetDateTime.class), anyLong(), anyString(), anyString());
    }

    @Test
    void rowsWhoseFirstTradeFieldsAreAllZeroAreMarkedAndTheRestAreNot() {
        String[] raws = {
                "{\"dataBody\":{\"acml_vlm\":\"0\",\"opn_prc\":\"0\"}}",       // 모두 0
                "{\"dataBody\":{\"acml_vlm\":\"0\",\"opn_prc\":\"0.00\"}}",    // 소수 0도 0
                "{\"dataBody\":{\"acml_vlm\":\"0\",\"opn_prc\":\"10000\"}}",   // 한쪽만 0
                "{\"dataBody\":{\"opn_prc\":\"0\"}}",                              // 칸이 없음
                "{\"dataBody\":{\"acml_vlm\":0,\"opn_prc\":0}}",                   // 숫자 0
                "{\"dataBody\":{\"acml_vlm\":\"abc\",\"opn_prc\":\"0\"}}"      // 숫자가 아님
        };
        for (int i = 0; i < raws.length; i++) {
            listener.onSnapshot(message("a5000000-0000-4000-8000-00000000000" + (i + 1), "ITS005", 10_000 + i,
                    "2026-03-05T11:00:0" + i + "+09:00", raws[i]));
        }
        PriceSnapshotRepository repository = new PriceSnapshotRepository(jdbc);
        OffsetDateTime from = OffsetDateTime.parse("2026-03-05T11:00:00+09:00");
        OffsetDateTime to = OffsetDateTime.parse("2026-03-05T11:01:00+09:00");

        List<PricePoint> points = repository.findBetween("ITS005", from, to, List.of("dataBody.acml_vlm", "dataBody.opn_prc"));

        assertThat(points).extracting(PricePoint::beforeFirstTrade).containsExactly(true, true, false, false, true, false);
        assertThat(points).extracting(PricePoint::price).containsExactly(10_000L, 10_001L, 10_002L, 10_003L, 10_004L, 10_005L);
        // 칸 목록이 비면 아무것도 표시하지 않는다.
        assertThat(repository.findBetween("ITS005", from, to, List.of())).extracting(PricePoint::beforeFirstTrade)
                .containsOnly(false);
    }

    @Test
    void latestRowsBeforeAPointAreReturnedInTimeOrder() {
        for (int i = 0; i < 4; i++) {
            listener.onSnapshot(message("a6000000-0000-4000-8000-00000000000" + (i + 1), "ITS006", 10_000 + i,
                    "2026-03-05T12:00:0" + i + "+09:00", "{\"price\":\"1\"}"));
        }

        List<PricePoint> points = new PriceSnapshotRepository(jdbc).findLatestBetween("ITS006",
                OffsetDateTime.parse("2026-03-05T11:59:00+09:00"), OffsetDateTime.parse("2026-03-05T12:00:03+09:00"), 2, List.of());

        // 끝(12:00:03) 미만에서 가장 가까운 두 건
        assertThat(points).extracting(PricePoint::price).containsExactly(10_001L, 10_002L);
    }

    private static boolean partitionExists(String name) {
        return jdbc.sql("SELECT to_regclass(:name) IS NOT NULL").param("name", name).query(Boolean.class).single();
    }

    static String withMarketSchema(String url) {
        return url + (url.contains("?") ? "&" : "?") + "currentSchema=market";
    }

    private static String message(String eventId, String symbol, long price, String asOf, String raw) {
        var mapper = JsonMapper.builder().build();
        return """
                {"eventId":"%s","eventType":"market.snapshot","schemaVersion":1,
                 "occurredAt":"%s","producedAt":"%s","correlationId":"it",
                 "payload":{"symbol":"%s","price":%d,"asOf":"%s","source":"simulator","raw":%s}}
                """.formatted(eventId, asOf, asOf, symbol, price, asOf, mapper.writeValueAsString(raw));
    }
}

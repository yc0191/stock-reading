package dev.stockreading.market.insight;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class InsightIntegrationTest {

    private static final String RAW_RESPONSE =
            "{\"dataBody\": {\"items\": [{\"close\": \" 10000 \", \"rate\": 1.10, \"big\": 12345678901234567890}]}, \"한글\": \"값\"}";

    private static JdbcClient jdbc;
    private static InsightListener listener;
    private static FailedEventRecoverer recoverer;
    private static OverseasCloseRateQuery closeRateQuery;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");

        // 서비스와 같은 classpath 마이그레이션을 적용한다(스키마는 기본값인 public).
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();

        jdbc = JdbcClient.create(new DriverManagerDataSource(url, user, password));
        listener = new InsightListener(new InsightRepository(jdbc), JsonMapper.builder().build());
        closeRateQuery = new OverseasCloseRateQuery(new InsightRepository(jdbc), JsonMapper.builder().build());
        recoverer = new FailedEventRecoverer(new FailedEventRepository(jdbc));
    }

    @Test
    void migrationCreatesTheInsightTablesAndFailedEvent() {
        List<String> tables = jdbc.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                .query(String.class).list();

        assertThat(tables).contains("daily_price", "world_index", "exchange_rate_summary", "market_summary",
                "overseas_daily_price", "failed_event", "price_snapshot");
    }

    @Test
    void sameEventIdReceivedTwiceLeavesOneRow() throws IOException {
        String message = message("11111111-1111-4111-8111-111111111111", "daily-price", "\"IT0001\"", RAW_RESPONSE);

        listener.onInsight(message);
        listener.onInsight(message);

        assertThat(count("daily_price", "event_id = CAST('11111111-1111-4111-8111-111111111111' AS uuid)")).isEqualTo(1);
    }

    @Test
    void everyApiTypeIsStoredInItsOwnTable() throws IOException {
        listener.onInsight(message("22222222-2222-4222-8222-222222222221", "world-index", "\"ASIA\"", RAW_RESPONSE));
        listener.onInsight(message("22222222-2222-4222-8222-222222222222", "exchange-rate-summary", "null", RAW_RESPONSE));
        listener.onInsight(message("22222222-2222-4222-8222-222222222223", "market-summary", "null", RAW_RESPONSE));

        assertThat(jdbc.sql("SELECT target FROM world_index WHERE event_id = CAST('22222222-2222-4222-8222-222222222221' AS uuid)")
                .query(String.class).single()).isEqualTo("ASIA");
        assertThat(count("exchange_rate_summary", "event_id = CAST('22222222-2222-4222-8222-222222222222' AS uuid)")).isEqualTo(1);
        assertThat(count("market_summary", "event_id = CAST('22222222-2222-4222-8222-222222222223' AS uuid)")).isEqualTo(1);
    }

    @Test
    void overseasDailyPriceIsStoredOnceWithItsTicker() throws IOException {
        String message = message("55555555-5555-4555-8555-555555555555", "overseas-daily-price", "\"ITESTB.X\"", RAW_RESPONSE);

        listener.onInsight(message);
        listener.onInsight(message);

        assertThat(jdbc.sql("SELECT target FROM overseas_daily_price WHERE event_id = CAST('55555555-5555-4555-8555-555555555555' AS uuid)")
                .query(String.class).single()).isEqualTo("ITESTB.X");
        assertThat(jdbc.sql("""
                        SELECT response #>> '{dataBody,items,0,rate}' FROM overseas_daily_price
                        WHERE event_id = CAST('55555555-5555-4555-8555-555555555555' AS uuid)
                        """).query(String.class).single()).isEqualTo("1.10");
    }

    @Test
    void closeRateIsReadFromTheStoredRawResponseOfTheTicker() throws IOException {
        // 10/21 16:30(한국)에 받은 행: 맨 앞은 진행 중인 10/21 막대, 그 다음이 10/20 정규장 마감이다.
        String first = "{\"dataBody\": {\"up_dwn_r_p2\": \"  9.99\", \"out2\": ["
                + "{\"dt\": \"20261021\", \"up_dwn_r_p2\": \"  0.10\"},"
                + "{\"dt\": \"20261020\", \"up_dwn_r_p2\": \"  1.25\"}]}}";
        // 다른 종목의 행은 읽지 않아야 한다.
        String other = "{\"dataBody\": {\"out2\": [{\"dt\": \"20261020\", \"up_dwn_r_p2\": \" -3.00\"}]}}";
        listener.onInsight(fetchedOn1021(message("66666666-6666-4666-8666-666666666661", "overseas-daily-price", "\"ITESTC\"", first)));
        listener.onInsight(fetchedOn1021(message("66666666-6666-4666-8666-666666666662", "overseas-daily-price", "\"ITESTD\"", other)));

        OverseasCloseRate rate = closeRateQuery.find("ITESTC", OffsetDateTime.parse("2026-10-21T08:00:00Z"))
                .orElseThrow();

        assertThat(rate.date()).isEqualTo("20261020");
        assertThat(rate.rate()).isEqualTo("  1.25");
    }

    @Test
    void rawResponseValuesSurviveInJsonb() throws IOException {
        listener.onInsight(message("33333333-3333-4333-8333-333333333333", "daily-price", "\"IT0002\"", RAW_RESPONSE));

        Map<String, Object> row = jdbc.sql("""
                        SELECT response #>> '{dataBody,items,0,close}' AS close,
                               response #>> '{dataBody,items,0,rate}' AS rate,
                               response #>> '{dataBody,items,0,big}' AS big,
                               response ->> '한글' AS korean,
                               fetched_at IS NOT NULL AS has_fetched_at,
                               received_at IS NOT NULL AS has_received_at
                        FROM daily_price WHERE event_id = CAST('33333333-3333-4333-8333-333333333333' AS uuid)
                        """)
                .query().singleRow();

        // 문자열의 공백, 숫자의 소수 자릿수와 큰 정수가 값 수준에서 그대로 남아야 한다.
        assertThat(row).containsEntry("close", " 10000 ").containsEntry("rate", "1.10")
                .containsEntry("big", "12345678901234567890").containsEntry("korean", "값")
                .containsEntry("has_fetched_at", true).containsEntry("has_received_at", true);
    }

    @Test
    void failedRecordIsStoredOnceEvenWhenRecoveredTwice() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("market.insight", 2, 41L, "daily-price:IT0003", "{bad");

        recoverer.accept(record, new IllegalStateException("처리 실패"));
        recoverer.accept(record, new IllegalStateException("처리 실패"));

        Map<String, Object> row = jdbc.sql("""
                        SELECT payload, exception_class, attempts FROM failed_event
                        WHERE topic = 'market.insight' AND partition_no = 2 AND record_offset = 41
                        """)
                .query().singleRow();
        assertThat(row).containsEntry("payload", "{bad")
                .containsEntry("exception_class", "java.lang.IllegalStateException")
                .containsEntry("attempts", InsightErrorHandling.DELIVERY_ATTEMPTS);
    }

    @Test
    void invalidResponseJsonFailsAndStoresNothing() throws IOException {
        String message = message("44444444-4444-4444-8444-444444444444", "daily-price", "\"IT0004\"", "not json");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> listener.onInsight(message)).isNotNull();
        assertThat(count("daily_price", "event_id = CAST('44444444-4444-4444-8444-444444444444' AS uuid)")).isZero();
    }

    private static String fetchedOn1021(String message) {
        return message.replace("\"fetchedAt\":\"2026-10-20T16:30:01.800+09:00\"", "\"fetchedAt\":\"2026-10-21T16:30:01.800+09:00\"");
    }

    private static long count(String table, String where) {
        return jdbc.sql("SELECT count(*) FROM " + table + " WHERE " + where).query(Long.class).single();
    }

    private static String message(String eventId, String apiType, String targetJson, String response) {
        var mapper = JsonMapper.builder().build();
        return """
                {"eventId":"%s","eventType":"market.insight","schemaVersion":1,
                 "occurredAt":"2026-10-20T16:30:01.800+09:00","producedAt":"2026-10-20T16:30:02.350+09:00",
                 "correlationId":"it","payload":{"apiType":"%s","target":%s,"response":%s,
                 "scheduledFor":"2026-10-20T16:30:00+09:00","fetchedAt":"2026-10-20T16:30:01.800+09:00"}}
                """.formatted(eventId, apiType, targetJson, mapper.writeValueAsString(response));
    }
}

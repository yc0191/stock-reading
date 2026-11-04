package dev.stockreading.market.snapshot;

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
 * 시세 조회 실패 기록을 실제 Postgres에서 저장 후 다시 읽어 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 market 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class QuoteFailureIntegrationTest {

    private static JdbcClient jdbc;
    private static QuoteFailureListener listener;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");

        // 서비스 설정(application.yml)과 같이 market 스키마에만 마이그레이션하고, 스키마는 만들지 않는다.
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("market").defaultSchema("market").createSchemas(false).load().migrate();

        jdbc = JdbcClient.create(new DriverManagerDataSource(SnapshotStorageIntegrationTest.withMarketSchema(url), user, password));
        listener = new QuoteFailureListener(new QuoteFailureRepository(jdbc), JsonMapper.builder().build());
    }

    @Test
    void sameEventIdReceivedTwiceLeavesOneRowWithEveryField() {
        String message = message("b1000000-0000-4000-8000-000000000001", "ITF001",
                "  {\"msg\": \" 잠시 후 \", \"rate\": 1.10}  ");

        listener.onFailure(message);
        listener.onFailure(message);

        assertThat(jdbc.sql("SELECT count(*) FROM quote_failure WHERE symbol = 'ITF001'").query(Long.class).single())
                .isEqualTo(1);
        Map<String, Object> row = jdbc.sql("""
                        SELECT correlation_id, failure_type, http_status, response_code, elapsed_ms, exception_type,
                               token_age_ms, token_remaining_ms, concurrent_calls, tick_succeeded, tick_failed,
                               occurred_at = CAST('2026-03-05T10:00:02+09:00' AS timestamptz) AS same_occurred_at,
                               scheduled_for = CAST('2026-03-05T10:00:00+09:00' AS timestamptz) AS same_scheduled_for,
                               response_raw ->> 'msg' AS msg, response_raw #>> '{rate}' AS rate
                        FROM quote_failure WHERE symbol = 'ITF001'
                        """).query().singleRow();
        assertThat(row).containsEntry("correlation_id", "it").containsEntry("failure_type", "HTTP_ERROR")
                .containsEntry("http_status", 500).containsEntry("response_code", "E1001")
                .containsEntry("elapsed_ms", 1450L).containsEntry("exception_type", "HttpStatusException")
                .containsEntry("token_age_ms", 3_600_000L).containsEntry("token_remaining_ms", 82_800_000L)
                .containsEntry("concurrent_calls", 3).containsEntry("tick_succeeded", 2).containsEntry("tick_failed", 1)
                .containsEntry("same_occurred_at", true).containsEntry("same_scheduled_for", true)
                // 원문은 trim 외에 가공하지 않는다.
                .containsEntry("msg", " 잠시 후 ").containsEntry("rate", "1.10");
    }

    @Test
    void failureWithoutResponseIsStoredWithEmptyOptionalColumns() {
        listener.onFailure("""
                {"eventId":"b2000000-0000-4000-8000-000000000001","eventType":"market.snapshot.failed","schemaVersion":1,
                 "occurredAt":"2026-03-05T10:00:02+09:00","producedAt":"2026-03-05T10:00:02+09:00","correlationId":"it",
                 "payload":{"symbol":"ITF002","failureType":"TICK_SKIPPED","scheduledFor":"2026-03-05T10:00:00+09:00"}}
                """);

        Map<String, Object> row = jdbc.sql("""
                        SELECT failure_type, http_status, token_age_ms, response_raw FROM quote_failure WHERE symbol = 'ITF002'
                        """).query().singleRow();
        assertThat(row).containsEntry("failure_type", "TICK_SKIPPED").containsEntry("http_status", null)
                .containsEntry("token_age_ms", null).containsEntry("response_raw", null);
    }

    @Test
    void fieldMismatchFailureIsStoredWithItsChecksAndResponse() {
        listener.onFailure("""
                {"eventId":"b3000000-0000-4000-8000-000000000001","eventType":"market.snapshot.failed","schemaVersion":1,
                 "occurredAt":"2026-03-05T10:00:02+09:00","producedAt":"2026-03-05T10:00:02+09:00","correlationId":"it",
                 "payload":{"symbol":"ITF003","failureType":"FIELD_MISMATCH","scheduledFor":"2026-03-05T10:00:00+09:00",
                            "httpStatus":200,"exceptionType":"FieldMismatch:CHANGE+RATE",
                            "responseRaw":"{\\"dataBody\\":{\\"price\\":\\"99999\\",\\"rate\\":\\"  -1.00\\"}}"}}
                """);

        Map<String, Object> row = jdbc.sql("""
                        SELECT failure_type, exception_type, response_raw #>> '{dataBody,rate}' AS rate
                        FROM quote_failure WHERE symbol = 'ITF003'
                        """).query().singleRow();
        assertThat(row).containsEntry("failure_type", "FIELD_MISMATCH")
                .containsEntry("exception_type", "FieldMismatch:CHANGE+RATE")
                // 원문 칸의 앞 공백까지 그대로 남는다.
                .containsEntry("rate", "  -1.00");
    }

    private static String message(String eventId, String symbol, String responseRaw) {
        var mapper = JsonMapper.builder().build();
        return """
                {"eventId":"%s","eventType":"market.snapshot.failed","schemaVersion":1,
                 "occurredAt":"2026-03-05T10:00:02+09:00","producedAt":"2026-03-05T10:00:02.040+09:00","correlationId":"it",
                 "payload":{"symbol":"%s","failureType":"HTTP_ERROR","scheduledFor":"2026-03-05T10:00:00+09:00",
                            "httpStatus":500,"responseCode":"E1001","elapsedMs":1450,"exceptionType":"HttpStatusException",
                            "tokenState":{"ageMs":3600000,"remainingMs":82800000},
                            "concurrentCalls":3,"tickSucceeded":2,"tickFailed":1,"responseRaw":%s}}
                """.formatted(eventId, symbol, mapper.writeValueAsString(responseRaw));
    }
}

package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.stockreading.market.event.DailyPayload;

/**
 * 시세처리기 운영 상태 조회의 실제 Postgres 통합 테스트. 세 환경변수가 모두 있을 때만 실행하며, 대상은 비어 있는 새 DB라고 가정한다.
 * 세는 범위 안의 모든 행이 대상이라 테스트마다 다른 해(다른 테스트 클래스와도 겹치지 않는 해)를 쓴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class OperationsIntegrationTest {

    private static JdbcClient jdbc;
    private static OperationsRepository repository;
    private static OperationsReader reader;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url, user, password));
        repository = new OperationsRepository(jdbc);
        reader = new OperationsReader(repository);
    }

    @Test
    void classifiesInsightRowsByTheKoreanTimeOfDayTheyWereReceived() {
        OffsetDateTime since = OffsetDateTime.parse("2031-03-03T16:00:00+09:00");
        OffsetDateTime asOf = OffsetDateTime.parse("2031-03-04T17:00:00+09:00");
        insertInsight("overseas_daily_price", "TESTA", "2031-03-04T08:30:05+09:00");
        insertInsight("world_index", "C", "2031-03-04T08:30:07+09:00");
        insertInsight("world_index", "S", "2031-03-04T08:59:59+09:00");
        insertInsight("exchange_rate_summary", null, "2031-03-04T09:00:00+09:00");
        insertInsight("market_summary", null, "2031-03-04T15:59:59+09:00");
        insertInsight("exchange_rate_summary", null, "2031-03-03T16:30:03+09:00");
        insertInsight("daily_price", "TEST01", "2031-03-03T16:30:01+09:00");

        Map<String, OperationsRepository.CollectionRow> rows = byKind(asOf, since);

        assertThat(rows.get("pre-open").count()).isEqualTo(3);
        assertThat(rows.get("pre-open").lastAt()).isEqualTo(OffsetDateTime.parse("2031-03-04T08:59:59+09:00"));
        assertThat(rows.get("intraday").count()).isEqualTo(2);
        assertThat(rows.get("post-close").count()).isEqualTo(2);
        assertThat(rows.get("post-close").firstAt()).isEqualTo(OffsetDateTime.parse("2031-03-03T16:30:01+09:00"));
        assertThat(rows).doesNotContainKey("quote-1s");
    }

    @Test
    void rangeEndsAreIncludedAndRowsOutsideAreLeftOut() {
        OffsetDateTime since = OffsetDateTime.parse("2032-03-03T17:00:00+09:00");
        OffsetDateTime asOf = OffsetDateTime.parse("2032-03-04T17:00:00+09:00");
        insertInsight("daily_price", "TEST01", "2032-03-03T16:59:59+09:00");
        insertInsight("daily_price", "TEST02", since.toString());
        insertInsight("daily_price", "TEST03", asOf.toString());
        insertInsight("daily_price", "TEST04", "2032-03-04T17:00:01+09:00");

        OperationsRepository.CollectionRow row = byKind(asOf, since).get("post-close");

        assertThat(row.count()).isEqualTo(2);
        assertThat(row.firstAt()).isEqualTo(since);
        assertThat(row.lastAt()).isEqualTo(asOf);
    }

    @Test
    void countsQuoteSnapshotsByReceivedTimeAndSplitsTheSkippedTickReasons() {
        OffsetDateTime since = OffsetDateTime.parse("2033-03-03T17:00:00+09:00");
        OffsetDateTime asOf = OffsetDateTime.parse("2033-03-04T17:00:00+09:00");
        createPartition("2033-03-04");
        insertQuote("2033-03-04T09:00:00+09:00", "2033-03-04T09:00:01+09:00");
        insertQuote("2033-03-04T09:00:01+09:00", "2033-03-04T09:00:02+09:00");
        insertQuote("2033-03-04T09:00:02+09:00", "2033-03-04T17:00:01+09:00");
        insertFailure("EMPTY_PRICE", null, "2033-03-04T10:00:00+09:00");
        insertFailure("EMPTY_PRICE", null, "2033-03-04T10:00:01+09:00");
        insertFailure("TICK_SKIPPED", "RateLimited", "2033-03-04T10:00:02+09:00");
        insertFailure("TICK_SKIPPED", "SupersededByNewerTick", "2033-03-04T10:00:03+09:00");
        insertFailure("TICK_SKIPPED", "Other", "2033-03-04T10:00:04+09:00");
        insertFailure("TIMEOUT", null, "2033-03-04T17:00:01+09:00");

        OperationsRepository.CollectionRow quotes = byKind(asOf, since).get("quote-1s");
        Map<String, Integer> failures = repository.quoteFailures(since, asOf);

        assertThat(quotes.count()).isEqualTo(2);
        assertThat(quotes.lastAt()).isEqualTo(OffsetDateTime.parse("2033-03-04T09:00:02+09:00"));
        assertThat(failures).containsEntry("EMPTY_PRICE", 2).containsEntry("TICK_SKIPPED_RATE_LIMITED", 1)
                .containsEntry("TICK_SKIPPED_SUPERSEDED", 1).containsEntry("TICK_SKIPPED_OTHER", 1)
                .doesNotContainKey("TIMEOUT");
        assertThat(reader.read(since, asOf).quoteFailures()).containsEntry("TIMEOUT", 0)
                .containsEntry("EMPTY_PRICE", 2);
    }

    @Test
    void countsDroppedMessagesPerTopicInsideTheRange() {
        OffsetDateTime since = OffsetDateTime.parse("2034-03-03T17:00:00+09:00");
        OffsetDateTime asOf = OffsetDateTime.parse("2034-03-04T17:00:00+09:00");
        insertDropped("market.snapshot", 1, "2034-03-04T10:00:00+09:00");
        insertDropped("market.snapshot", 2, "2034-03-04T10:00:01+09:00");
        insertDropped("market.insight", 3, "2034-03-04T11:00:00+09:00");
        insertDropped("market.insight", 4, "2034-03-04T17:00:01+09:00");

        assertThat(repository.droppedMessages(since, asOf)).containsExactly(
                new OperationsRepository.DroppedRow("market.insight", 1),
                new OperationsRepository.DroppedRow("market.snapshot", 2));
    }

    @Test
    void emptyRangeGivesZeroEverywhereAndDoesNotWrite() {
        OffsetDateTime since = OffsetDateTime.parse("2035-03-03T17:00:00+09:00");
        OffsetDateTime asOf = OffsetDateTime.parse("2035-03-04T17:00:00+09:00");
        long before = jdbc.sql("SELECT (SELECT count(*) FROM quote_failure) + (SELECT count(*) FROM failed_event)")
                .query(Long.class).single();

        DailyPayload.Operations operations = reader.read(since, asOf);

        assertThat(operations.collections()).allSatisfy(c -> assertThat(c.receivedCount()).isZero());
        assertThat(operations.quoteFailures().values()).containsOnly(0);
        assertThat(operations.droppedMessages()).isEmpty();
        assertThat(jdbc.sql("SELECT (SELECT count(*) FROM quote_failure) + (SELECT count(*) FROM failed_event)")
                .query(Long.class).single()).isEqualTo(before);
    }

    private static Map<String, OperationsRepository.CollectionRow> byKind(OffsetDateTime asOf, OffsetDateTime since) {
        return repository.collections(since, asOf).stream().collect(
                java.util.stream.Collectors.toMap(OperationsRepository.CollectionRow::kind, row -> row));
    }

    private static void insertInsight(String table, String target, String fetchedAt) {
        String columns = target == null ? "event_id, response, fetched_at" : "event_id, target, response, fetched_at";
        String values = target == null ? ":eventId, CAST('{}' AS jsonb), :fetchedAt"
                : ":eventId, :target, CAST('{}' AS jsonb), :fetchedAt";
        var statement = jdbc.sql("INSERT INTO " + table + " (" + columns + ") VALUES (" + values + ")")
                .param("eventId", UUID.randomUUID()).param("fetchedAt", OffsetDateTime.parse(fetchedAt));
        if (target != null) {
            statement = statement.param("target", target);
        }
        statement.update();
    }

    private static void createPartition(String day) {
        jdbc.sql("CREATE TABLE IF NOT EXISTS price_snapshot_p" + day.replace("-", "")
                + " PARTITION OF price_snapshot FOR VALUES FROM ('" + day + " 00:00:00+09') TO ('"
                + OffsetDateTime.parse(day + "T00:00:00+09:00").plusDays(1) + "')").update();
    }

    private static void insertQuote(String asOf, String receivedAt) {
        jdbc.sql("INSERT INTO price_snapshot (event_id, symbol, as_of, price, received_at) "
                        + "VALUES (:eventId, 'TEST01', :asOf, 100, :receivedAt)")
                .param("eventId", UUID.randomUUID()).param("asOf", OffsetDateTime.parse(asOf))
                .param("receivedAt", OffsetDateTime.parse(receivedAt)).update();
    }

    private static void insertFailure(String type, String exceptionType, String receivedAt) {
        OffsetDateTime at = OffsetDateTime.parse(receivedAt);
        jdbc.sql("INSERT INTO quote_failure (event_id, correlation_id, symbol, failure_type, occurred_at, "
                        + "scheduled_for, exception_type, received_at) "
                        + "VALUES (:eventId, 'c', 'TEST01', :type, :at, :at, :exceptionType, :at)")
                .param("eventId", UUID.randomUUID()).param("type", type).param("exceptionType", exceptionType)
                .param("at", at).update();
    }

    private static void insertDropped(String topic, long offset, String failedAt) {
        jdbc.sql("INSERT INTO failed_event (topic, partition_no, record_offset, exception_class, attempts, failed_at) "
                        + "VALUES (:topic, 0, :offset, 'X', 1, :failedAt)")
                .param("topic", topic).param("offset", offset).param("failedAt", OffsetDateTime.parse(failedAt))
                .update();
    }
}

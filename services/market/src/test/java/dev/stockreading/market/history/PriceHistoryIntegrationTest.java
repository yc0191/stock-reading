package dev.stockreading.market.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.stockreading.market.event.HistoryPayload;
import tools.jackson.databind.json.JsonMapper;

/**
 * 시세 이력 조회를 실제 Postgres에 저장된 스냅샷으로 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 market 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class PriceHistoryIntegrationTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-03-10T10:00:00+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-03-10T10:05:00+09:00");
    private static final OffsetDateTime THIN_SINCE = OffsetDateTime.parse("2026-03-10T11:00:00+09:00");

    private static JdbcClient jdbc;
    private static PriceHistoryRepository repository;

    @BeforeAll
    static void migrateAndSeed() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");

        // 서비스 설정(application.yml)과 같이 market 스키마에만 마이그레이션하고, 스키마는 만들지 않는다.
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("market").defaultSchema("market").createSchemas(false).load().migrate();

        jdbc = JdbcClient.create(new DriverManagerDataSource(url + (url.contains("?") ? "&" : "?") + "currentSchema=market",
                user, password));
        repository = new PriceHistoryRepository(jdbc);

        // 이 패키지는 쓰기 경로가 없으므로 스냅샷과 그날 파티션을 SQL로 직접 넣는다.
        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS price_snapshot_p20260310 PARTITION OF price_snapshot
                FOR VALUES FROM ('2026-03-10T00:00:00+09:00') TO ('2026-03-11T00:00:00+09:00')
                """).update();
        insert("ITH001", SINCE.minusSeconds(1), 9_999);
        insert("ITH001", SINCE, 10_000);
        insert("ITH001", SINCE.plusMinutes(1), 10_010);
        insert("ITH001", SINCE.plusMinutes(2), 10_020);
        insert("ITH001", UNTIL, 10_050);
        insert("ITH001", UNTIL.plusSeconds(1), 10_051);
        // 다른 종목의 행은 읽지 않아야 한다.
        insert("ITH002", SINCE.plusMinutes(1), 20_000);
        // 간격 솎아내기용: 1초마다 한 점, 35초 동안. 가격은 순번이라 어느 점이 남았는지 알 수 있다.
        for (int i = 0; i < 35; i++) {
            insert("ITH003", THIN_SINCE.plusSeconds(i), 10_000 + i);
        }
    }

    @Test
    void rangeIncludesBothEndsInTimeOrderAndOnlyTheSymbol() {
        List<PriceHistoryRepository.StoredPoint> rows = repository.findBetweenInclusive("ITH001", SINCE, UNTIL, 100);

        assertThat(rows).extracting(PriceHistoryRepository.StoredPoint::price)
                .containsExactly(10_000L, 10_010L, 10_020L, 10_050L);
        assertThat(rows.get(0).asOf().toInstant()).isEqualTo(SINCE.toInstant());
        assertThat(rows.get(3).asOf().toInstant()).isEqualTo(UNTIL.toInstant());
    }

    @Test
    void limitKeepsTheEarliestRows() {
        assertThat(repository.findBetweenInclusive("ITH001", SINCE, UNTIL, 2))
                .extracting(PriceHistoryRepository.StoredPoint::price).containsExactly(10_000L, 10_010L);
    }

    @Test
    void responseIsCutAtMaxPointsAndMarkedTruncated() {
        HistoryQuery listener = new HistoryQuery(repository);

        HistoryPayload answer = answer(listener, request(3));
        assertThat(answer.truncated()).isTrue();
        assertThat(answer.jobId()).isEqualTo("it-job");
        List<HistoryPayload.Point> points = answer.points();
        assertThat(points).containsExactly(
                new HistoryPayload.Point("2026-03-10T10:00:00+09:00", 10_000L),
                new HistoryPayload.Point("2026-03-10T10:01:00+09:00", 10_010L),
                new HistoryPayload.Point("2026-03-10T10:02:00+09:00", 10_020L));
    }

    @Test
    void responseWithinMaxPointsIsNotTruncated() {
        HistoryQuery listener = new HistoryQuery(repository);

        HistoryPayload answer = answer(listener, request(4));
        assertThat(answer.truncated()).isFalse();
        assertThat(answer.jobId()).isEqualTo("it-job");
        List<HistoryPayload.Point> points = answer.points();
        assertThat(points).hasSize(4);
    }

    @Test
    void thinnedQueryKeepsTheFirstPointOfEveryIntervalFromSince() {
        List<PriceHistoryRepository.StoredPoint> points =
                repository.findThinnedBetweenInclusive("ITH003", THIN_SINCE, THIN_SINCE.plusSeconds(34), 10, 100);

        assertThat(points).extracting(PriceHistoryRepository.StoredPoint::price)
                .containsExactly(10_000L, 10_010L, 10_020L, 10_030L);
    }

    @Test
    void thinnedQueryStopsAtTheLimitFromTheFront() {
        List<PriceHistoryRepository.StoredPoint> points =
                repository.findThinnedBetweenInclusive("ITH003", THIN_SINCE, THIN_SINCE.plusSeconds(34), 10, 2);

        assertThat(points).extracting(PriceHistoryRepository.StoredPoint::price).containsExactly(10_000L, 10_010L);
    }

    @Test
    void unthinnedQueryStillReturnsEveryPointWithBothEndsIncluded() {
        List<PriceHistoryRepository.StoredPoint> points =
                repository.findBetweenInclusive("ITH003", THIN_SINCE.plusSeconds(5), THIN_SINCE.plusSeconds(9), 100);

        assertThat(points).extracting(PriceHistoryRepository.StoredPoint::price)
                .containsExactly(10_005L, 10_006L, 10_007L, 10_008L, 10_009L);
    }

    private static void insert(String symbol, OffsetDateTime asOf, long price) {
        jdbc.sql("""
                        INSERT INTO price_snapshot (event_id, symbol, as_of, price, raw)
                        VALUES (gen_random_uuid(), :symbol, :asOf, :price, NULL)
                        """)
                .param("symbol", symbol).param("asOf", asOf).param("price", price).update();
    }

    private static String request(int maxPoints) {
        return """
                {"eventId":"c1000000-0000-4000-8000-000000000001","eventType":"market.history.requested","schemaVersion":1,
                 "occurredAt":"2026-03-10T10:10:00+09:00","producedAt":"2026-03-10T10:10:00+09:00","correlationId":"it",
                 "payload":{"jobId":"it-job","seq":1,"symbol":"ITH001",
                            "since":"2026-03-10T10:00:00+09:00","until":"2026-03-10T10:05:00+09:00","maxPoints":%d}}
                """.formatted(maxPoints);
    }

    // 요청 이벤트 모양의 JSON 에서 payload 를 꺼내 내부 조회 API 요청 본문으로 조회 클래스에 넣는다.
    private static HistoryPayload answer(HistoryQuery query, String request) {
        JsonMapper mapper = JsonMapper.builder().build();
        return query.answer(mapper.treeToValue(mapper.readTree(request).get("payload"), HistoryRequest.class));
    }
}

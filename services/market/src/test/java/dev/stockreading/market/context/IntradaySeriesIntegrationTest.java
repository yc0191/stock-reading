package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import java.time.OffsetDateTime;
import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.stockreading.market.event.DailyPayload;
import tools.jackson.databind.json.JsonMapper;

/**
 * 그래프용 장중 흐름의 1분 솎기와 계산을 실제 Postgres에 저장한 스냅샷으로 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 market 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class IntradaySeriesIntegrationTest {

    // 다른 통합 테스트(03-04·03-10·03-12·03-13 파티션, ITH·OPF·IQF 종목)와 날짜·종목이 겹치지 않게 03-16과 ISR 종목을 쓴다.
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-03-16T09:00:00+09:00");
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-03-16T09:10:00+09:00");
    private static final String RAW = "{\"dataBody\":{\"bdy_cls_prc\":\"10000\",\"sprc\":\"10000\",\"krx_bdy_clpr\":\"10000\"}}";

    private static JdbcClient jdbc;
    private static IntradaySeriesRepository repository;
    private static OpenFlowRepository snapshots;

    @BeforeAll
    static void migrateAndSeed() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");

        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("market").defaultSchema("market").createSchemas(false).load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url + (url.contains("?") ? "&" : "?") + "currentSchema=market",
                user, password));
        repository = new IntradaySeriesRepository(jdbc);
        snapshots = new OpenFlowRepository(jdbc);

        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS price_snapshot_p20260316 PARTITION OF price_snapshot
                FOR VALUES FROM ('2026-03-16T00:00:00+09:00') TO ('2026-03-17T00:00:00+09:00')
                """).update();
        // 구간 시작 전 시세는 쓰지 않는다.
        insert("ISR001", SINCE.minusSeconds(1), 9_900, null, SINCE.minusSeconds(1));
        // 09:00 분: 세 건 중 시세 시각이 가장 늦은 10_020만 남는다.
        insert("ISR001", SINCE.plusSeconds(1), 10_000, null, SINCE.plusSeconds(1));
        insert("ISR001", SINCE.plusSeconds(30), 10_010, null, SINCE.plusSeconds(30));
        insert("ISR001", SINCE.plusSeconds(59), 10_020, null, SINCE.plusSeconds(59));
        // 09:01 분: 가격 0은 쓰지 않는다.
        insert("ISR001", SINCE.plusSeconds(61), 10_100, null, SINCE.plusSeconds(61));
        insert("ISR001", SINCE.plusSeconds(62), 0, null, SINCE.plusSeconds(62));
        // 09:09 분: 판 시작 뒤에 받은 행은 쓰지 않는다.
        insert("ISR001", AS_OF.minusSeconds(2), 10_300, RAW, AS_OF.minusSeconds(2));
        insert("ISR001", AS_OF.minusSeconds(1), 10_900, RAW, AS_OF.plusSeconds(1));
        // 시세 시각이 판 시작 뒤인 행도 쓰지 않는다.
        insert("ISR001", AS_OF.plusSeconds(1), 11_000, RAW, AS_OF.plusSeconds(1));
        // 다른 종목의 행은 읽지 않아야 한다.
        insert("ISR002", SINCE.plusSeconds(5), 20_000, null, SINCE.plusSeconds(5));
    }

    @Test
    void eachMinuteGivesItsLastQuoteReceivedByAsOfNewestFirst() {
        List<IntradaySeriesPort.Point> points = repository.minuteLasts("ISR001", SINCE, AS_OF, 10);

        // DB는 시각을 UTC로 돌려주므로 같은 순간인지로 비교한다.
        assertThat(points).extracting(p -> p.asOf().toInstant(), IntradaySeriesPort.Point::price)
                .containsExactly(tuple(AS_OF.minusSeconds(2).toInstant(), 10_300L),
                        tuple(SINCE.plusSeconds(61).toInstant(), 10_100L),
                        tuple(SINCE.plusSeconds(59).toInstant(), 10_020L));
    }

    @Test
    void theLimitKeepsTheLatestMinutesAndAnotherSymbolOrAnEmptyRangeGivesNothing() {
        assertThat(repository.minuteLasts("ISR001", SINCE, AS_OF, 2)).extracting(IntradaySeriesPort.Point::price)
                .containsExactly(10_300L, 10_100L);
        assertThat(repository.minuteLasts("ISR003", SINCE, AS_OF, 10)).isEmpty();
        assertThat(repository.minuteLasts("ISR001", AS_OF.plusMinutes(1), AS_OF.plusMinutes(2), 10)).isEmpty();
    }

    @Test
    void storedRowsGoThroughTheCalculatorToTheDocumentedRates() {
        IntradaySeriesCalculator calculator = new IntradaySeriesCalculator(repository, snapshots,
                JsonMapper.builder().build(),
                List.of("dataBody.bdy_cls_prc", "dataBody.sprc", "dataBody.krx_bdy_clpr"));

        DailyPayload.IntradaySeries series = calculator.calculate("ISR001", SINCE, AS_OF, 420);

        assertThat(series.prevClose()).isEqualTo("10000");
        assertThat(series.points()).extracting(DailyPayload.IntradayPoint::at,
                        DailyPayload.IntradayPoint::rateVsPrevClose)
                .containsExactly(tuple("2026-03-16T09:00:59+09:00", "0.20"), tuple("2026-03-16T09:01:01+09:00", "1.00"),
                        tuple("2026-03-16T09:09:58+09:00", "3.00"));
        assertThat(calculator.calculate("ISR003", SINCE, AS_OF, 420)).isEqualTo(DailyPayload.IntradaySeries.none());
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

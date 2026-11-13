package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

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
 * 일봉 없는 ETF의 하루 등락 대체값에 쓸 장중 시세 조회를 실제 Postgres에 저장한 스냅샷으로 확인한다. 세 환경변수가 모두 있을 때만
 * 실행하며, 대상은 market 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class IntradayQuoteIntegrationTest {

    // 다른 통합 테스트(03-04·03-10·03-12 파티션, ITH·OPF 종목)와 날짜·종목이 겹치지 않게 03-13과 IQF 종목을 쓴다.
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-03-12T17:00:00+09:00");
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-03-13T16:48:00+09:00");
    private static final String RAW = "{\"dataBody\":{\"bdy_cls_prc\":\"10000\",\"sprc\":\"10000\",\"krx_bdy_clpr\":\"10000\"}}";

    private static JdbcClient jdbc;
    private static IntradayQuoteRepository repository;

    @BeforeAll
    static void migrateAndSeed() {
        String url = System.getenv("MARKET_IT_DB_URL");
        String user = System.getenv("MARKET_IT_DB_USER");
        String password = System.getenv("MARKET_IT_DB_PASSWORD");

        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("market").defaultSchema("market").createSchemas(false).load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url + (url.contains("?") ? "&" : "?") + "currentSchema=market",
                user, password));
        repository = new IntradayQuoteRepository(jdbc);

        jdbc.sql("""
                CREATE TABLE IF NOT EXISTS price_snapshot_p20260313 PARTITION OF price_snapshot
                FOR VALUES FROM ('2026-03-13T00:00:00+09:00') TO ('2026-03-14T00:00:00+09:00')
                """).update();
        OffsetDateTime close = OffsetDateTime.parse("2026-03-13T15:29:58+09:00");
        insert("IQF001", close.minusMinutes(1), 9_800, RAW, close.minusMinutes(1));
        insert("IQF001", close, 9_705, RAW, close);
        // 시세 시각은 범위 안이지만 판 시각 뒤에 받은 행: 쓰지 않아야 한다.
        insert("IQF001", close.plusSeconds(1), 9_000, RAW, AS_OF.plusSeconds(1));
        // 다른 종목의 행은 읽지 않아야 한다.
        insert("IQF002", close, 20_000, RAW, close);
    }

    @Test
    void readsTheLastQuoteReceivedInsideTheRangeAndNotOneReceivedAfterTheEdition() {
        IntradayQuotePort.Quote quote = repository.lastReceived("IQF001", SINCE, AS_OF).orElseThrow();

        assertThat(quote.price()).isEqualTo(9_705L);
        assertThat(quote.receivedAt()).isEqualTo(OffsetDateTime.parse("2026-03-13T15:29:58+09:00"));
        assertThat(quote.raw()).contains("\"bdy_cls_prc\"");
        assertThat(repository.lastReceived("IQF003", SINCE, AS_OF)).isEmpty();
        assertThat(repository.lastReceived("IQF001", AS_OF.minusMinutes(10), AS_OF)).isEmpty();
    }

    @Test
    void storedRowsGoThroughTheFallbackToTheDocumentedRate() {
        DayRateFallback fallback = new DayRateFallback(repository, JsonMapper.builder().build(),
                List.of("dataBody.bdy_cls_prc", "dataBody.sprc", "dataBody.krx_bdy_clpr"));

        DailyPayload.Etf etf = fallback.apply(DailyPayload.Etf.withoutDaily("IQF001",
                DailyPayload.NO_DAILY_PRICE_IN_RANGE, List.of(), false, null), SINCE, AS_OF);

        assertThat(etf.rate()).isEqualTo("-2.95");
        assertThat(etf.prevClose()).isEqualTo("10000");
        assertThat(etf.intradayFallback()).isEqualTo(
                new DailyPayload.IntradayFallback("9705", "2026-03-13T15:29:58+09:00"));
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

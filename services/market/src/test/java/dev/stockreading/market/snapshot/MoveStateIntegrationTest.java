package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.stockreading.market.MarketProperties;

/**
 * 변동 상태(재발행 판단용)를 실제 Postgres에 저장하고 다시 읽어 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 market 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class MoveStateIntegrationTest {

    private static final OffsetDateTime T0 = OffsetDateTime.parse("2026-03-05T10:00:00+09:00");

    private static String url;
    private static String user;
    private static String password;

    @BeforeAll
    static void migrate() {
        url = System.getenv("MARKET_IT_DB_URL");
        user = System.getenv("MARKET_IT_DB_USER");
        password = System.getenv("MARKET_IT_DB_PASSWORD");

        // 서비스 설정(application.yml)과 같이 market 스키마에만 마이그레이션하고, 스키마는 만들지 않는다.
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("market").defaultSchema("market").createSchemas(false).load().migrate();
    }

    @Test
    void savedStateIsReadBackByARepositoryOnANewConnection() {
        repository().save("ITM001", DetectionType.WINDOW, 10_300, T0, T0.plusMinutes(1));

        // 재시작을 흉내 내어 새 연결 위의 새 저장소로 읽는다.
        assertState(repository().find("ITM001", DetectionType.WINDOW), 10_300, T0);
        assertThat(repository().find("ITM001", DetectionType.PREV_CLOSE)).isEmpty();
    }

    @Test
    void savingAgainKeepsOneRowWithTheLatestValuesAndDeleteRemovesIt() {
        MoveStateRepository repository = repository();
        repository.save("ITM002", DetectionType.PREV_CLOSE, 10_300, T0, T0.plusMinutes(1));
        repository.save("ITM002", DetectionType.PREV_CLOSE, 10_500, T0, T0.plusMinutes(3));

        assertThat(count("ITM002")).isEqualTo(1);
        assertState(repository().find("ITM002", DetectionType.PREV_CLOSE), 10_500, T0);
        assertThat(jdbc().sql("""
                        SELECT last_published_at = CAST(:at AS timestamptz) FROM move_state
                        WHERE symbol = 'ITM002' AND detection_type = 'prevClose'
                        """).param("at", T0.plusMinutes(3)).query(Boolean.class).single()).isTrue();

        repository.delete("ITM002", DetectionType.PREV_CLOSE);

        assertThat(count("ITM002")).isZero();
    }

    @Test
    void trackerStartedAgainDoesNotRepublishAMoveThatWasAlreadyPublished() {
        MoveDetector detector = mock(MoveDetector.class);
        MovePublisher publisher = mock(MovePublisher.class);
        Detection first = new Detection(DetectionType.WINDOW, 10_000, 10_200, new BigDecimal("2.00"), new BigDecimal("1.00"), T0);
        Detection small = new Detection(DetectionType.WINDOW, 10_000, 10_250, new BigDecimal("2.50"), new BigDecimal("1.00"), T0);
        when(detector.evaluate("ITM003", T0, 10_200, null)).thenReturn(List.of(Evaluation.detected(first)));
        when(detector.evaluate("ITM003", T0.plusMinutes(1), 10_250, null)).thenReturn(List.of(Evaluation.detected(small)));

        new MoveTracker(detector, repository(), publisher, properties()).onSnapshot("ITM003", T0, 10_200, null, "it");
        // 재시작: 새 연결의 저장소를 붙인 새 추적기. 마지막 발행 가격(10200) 대비 0.49%라 다시 발행하지 않는다.
        new MoveTracker(detector, repository(), publisher, properties()).onSnapshot("ITM003", T0.plusMinutes(1), 10_250, null, "it");

        verify(publisher, times(1)).publish(eq("ITM003"), any(Detection.class), any(OffsetDateTime.class), any(OffsetDateTime.class), anyString());
        verify(publisher, never()).publish(eq("ITM003"), eq(small), any(OffsetDateTime.class), any(OffsetDateTime.class), anyString());
        assertState(repository().find("ITM003", DetectionType.WINDOW), 10_200, T0);
    }

    @Test
    void lastPublishIsKeptPerSymbolAndSurvivesClearingTheTypeStates() {
        MoveStateRepository repository = repository();
        repository.save("ITM004", DetectionType.WINDOW, 10_300, T0, T0.plusMinutes(1));
        assertThat(repository().findLastPublish("ITM004")).isEmpty();

        repository.saveLastPublish("ITM004", 10_300, T0.plusMinutes(1));
        repository.saveLastPublish("ITM004", 10_500, T0.plusMinutes(3));
        repository.delete("ITM004", DetectionType.WINDOW);

        // 새 연결로 읽는다. 종목마다 한 행이고 최신 값이며, 유형별 상태를 지워도 남는다.
        Optional<LastPublish> last = repository().findLastPublish("ITM004");
        assertThat(last).isPresent();
        assertThat(last.get().price()).isEqualTo(10_500L);
        assertThat(last.get().publishedAt().toInstant()).isEqualTo(T0.plusMinutes(3).toInstant());
        assertThat(count("ITM004")).isEqualTo(1);
        assertThat(repository().find("ITM004", DetectionType.WINDOW)).isEmpty();
    }

    @Test
    void trackerStartedAgainKeepsTheIntervalBetweenMovesOfTheSameSymbol() {
        MoveDetector detector = mock(MoveDetector.class);
        MovePublisher publisher = mock(MovePublisher.class);
        Detection first = new Detection(DetectionType.WINDOW, 10_000, 10_200, new BigDecimal("2.00"), new BigDecimal("1.00"), T0);
        Detection second = new Detection(DetectionType.PREV_CLOSE, 10_000, 10_320, new BigDecimal("3.20"), new BigDecimal("3.00"), T0);
        when(detector.evaluate("ITM005", T0, 10_200, null)).thenReturn(List.of(Evaluation.detected(first)));
        when(detector.evaluate("ITM005", T0.plusMinutes(10), 10_320, null)).thenReturn(List.of(Evaluation.detected(second)));

        new MoveTracker(detector, repository(), publisher, properties()).onSnapshot("ITM005", T0, 10_200, null, "it");
        // 재시작한 새 추적기. 10분 뒤 다른 유형이 +1.18%(마지막 발행 가격 대비 2% 미만)로 감지돼도 30분 간격 안이라 내지 않는다.
        new MoveTracker(detector, repository(), publisher, properties()).onSnapshot("ITM005", T0.plusMinutes(10), 10_320, null, "it");

        verify(publisher, times(1)).publish(eq("ITM005"), any(Detection.class), any(OffsetDateTime.class), any(OffsetDateTime.class), anyString());
    }

    private static MarketProperties properties() {
        return new MarketProperties(5, new BigDecimal("1.00"), List.of("fake.a"), new BigDecimal("3.00"), List.of(),
                1, List.of("fake.volume", "fake.open"), 30, new BigDecimal("2.00"));
    }

    // DB는 시각을 UTC 오프셋으로 돌려주므로 같은 순간인지로 비교한다.
    private static void assertState(Optional<MoveState> state, long price, OffsetDateTime startedAt) {
        assertThat(state).isPresent();
        assertThat(state.get().lastPublishedPrice()).isEqualTo(price);
        assertThat(state.get().moveStartedAt().toInstant()).isEqualTo(startedAt.toInstant());
    }

    private static long count(String symbol) {
        return jdbc().sql("SELECT count(*) FROM move_state WHERE symbol = :symbol").param("symbol", symbol)
                .query(Long.class).single();
    }

    private static MoveStateRepository repository() {
        return new MoveStateRepository(jdbc());
    }

    private static JdbcClient jdbc() {
        return JdbcClient.create(new DriverManagerDataSource(SnapshotStorageIntegrationTest.withMarketSchema(url), user, password));
    }
}

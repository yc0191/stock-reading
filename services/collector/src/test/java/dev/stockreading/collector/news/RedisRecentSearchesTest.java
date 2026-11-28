package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 운영 보관소(RedisRecentSearches)를 진짜 Redis 로 확인한다. RecentSearchReuseTest 의 테스트를 모두 물려받아 같은 끝 기준을
 * 다시 돌리고, Redis 에서만 생기는 일(만료, 재시작 뒤 남음, 키 모양, Redis 장애)을 더 본다. Docker 가 없으면 건너뛴다.
 */
@Testcontainers(disabledWithoutDocker = true)
class RedisRecentSearchesTest extends RecentSearchReuseTest {

    // 운영 compose(infra/deploy/compose/services.yml)의 redis 와 같은 이미지.
    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8.2.2-alpine").withExposedPorts(6379);

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;

    private static final Clock FIXED = Clock.fixed(Instant.parse("2026-10-22T07:10:00Z"), ZoneId.of("Asia/Seoul"));

    @BeforeAll
    static void connect() {
        factory = connectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        redis = new StringRedisTemplate(factory);
    }

    @AfterAll
    static void disconnect() {
        factory.destroy();
    }

    @BeforeEach
    void emptyRedis() {
        redis.execute(connection -> {
            connection.serverCommands().flushAll();
            return null;
        }, true);
    }

    private static LettuceConnectionFactory connectionFactory(String host, int port) {
        LettuceConnectionFactory created = new LettuceConnectionFactory(new RedisStandaloneConfiguration(host, port),
                LettuceClientConfiguration.builder().commandTimeout(Duration.ofMillis(500)).build());
        created.afterPropertiesSet();
        created.start();
        return created;
    }

    private static RecentSearchProperties properties(Duration waitLimit, Duration lockTtl) {
        return new RecentSearchProperties(WINDOW, waitLimit, lockTtl, Duration.ofMillis(20));
    }

    @Override
    RecentSearchesPort recentSearches(Clock clock, Duration waitLimit) {
        return new RedisRecentSearches(redis, clock, properties(waitLimit, Duration.ofSeconds(30)));
    }

    @Override
    RecentSearchesPort recordingJustBeforeFirstClaim(Clock clock, List<NewsItem> other) {
        return new RedisRecentSearches(redis, clock, properties(Duration.ZERO, Duration.ofSeconds(30))) {
            private boolean first = true;

            @Override
            public boolean claim(String key) {
                if (first) {
                    first = false;
                    record(key, other);
                }
                return super.claim(key);
            }
        };
    }

    private static NewsItem article() {
        return new NewsItem("기사", "요약", "https://a.example.com/1", "", OffsetDateTime.parse("2026-10-22T15:00:00+09:00"),
                "example.com", new LinkedHashSet<>(List.of("TEST02", "TEST01")), new LinkedHashSet<>(Set.of("테스트검색어")));
    }

    @Test
    void storedKeysCarryOnlyAHashOfTheQueryAndAlwaysExpire() {
        RedisRecentSearches recent = new RedisRecentSearches(redis, FIXED, properties(Duration.ZERO, Duration.ofSeconds(30)));

        assertThat(recent.claim("테스트검색어")).isTrue();
        Set<String> whileSearching = redis.keys("*");
        recent.record("테스트검색어", List.of(article()));
        Set<String> afterRecord = redis.keys("*");

        String hash = RedisRecentSearches.hash("테스트검색어");
        assertThat(whileSearching).containsExactly(RedisRecentSearches.LOCK_PREFIX + hash);
        assertThat(afterRecord).containsExactly(RedisRecentSearches.RESULT_PREFIX + hash);
        assertThat(redis.getExpire(RedisRecentSearches.RESULT_PREFIX + hash)).isBetween(1L, WINDOW.toSeconds());
    }

    @Test
    void resultSurvivesACollectorRestartWithItsOffsetAndOrder() {
        new RedisRecentSearches(redis, FIXED, properties(Duration.ZERO, Duration.ofSeconds(30)))
                .record("테스트검색어", List.of(article()));

        RedisRecentSearches restarted = new RedisRecentSearches(redis, FIXED, properties(Duration.ZERO, Duration.ofSeconds(30)));

        assertThat(restarted.find("테스트검색어")).contains(List.of(article()));
        assertThat(restarted.find("테스트검색어").orElseThrow().get(0).symbols()).containsExactly("TEST02", "TEST01");
    }

    @Test
    void searchingMarkOfACollectorThatDiedFreesItselfAfterTheLockTtl() throws Exception {
        RedisRecentSearches died = new RedisRecentSearches(redis, FIXED, properties(Duration.ZERO, Duration.ofMillis(300)));
        RedisRecentSearches other = new RedisRecentSearches(redis, FIXED, properties(Duration.ZERO, Duration.ofMillis(300)));

        assertThat(died.claim("테스트검색어")).isTrue();
        assertThat(other.claim("테스트검색어")).isFalse();
        Thread.sleep(500);

        assertThat(other.claim("테스트검색어")).isTrue();
    }

    @Test
    void releaseAfterTheLockExpiredDoesNotFreeTheMarkSomeoneElseHolds() throws Exception {
        RedisRecentSearches slow = new RedisRecentSearches(redis, FIXED, properties(Duration.ZERO, Duration.ofMillis(300)));
        RedisRecentSearches other = new RedisRecentSearches(redis, FIXED, properties(Duration.ZERO, Duration.ofSeconds(30)));

        assertThat(slow.claim("테스트검색어")).isTrue();
        Thread.sleep(500);
        assertThat(other.claim("테스트검색어")).isTrue();
        slow.release("테스트검색어");

        assertThat(redis.hasKey(RedisRecentSearches.LOCK_PREFIX + RedisRecentSearches.hash("테스트검색어"))).isTrue();
    }

    @Test
    void unreachableRedisActsEmptySoTheCollectorJustSearches() {
        // 열려 있지 않은 포트를 가리켜 Redis 가 죽은 상황을 만든다.
        LettuceConnectionFactory dead = connectionFactory("127.0.0.1", 1);
        try {
            RedisRecentSearches recent = new RedisRecentSearches(new StringRedisTemplate(dead), FIXED,
                    properties(Duration.ofSeconds(5), Duration.ofSeconds(30)));

            long started = System.nanoTime();
            RecentSearchesPort.Turn turn = recent.take("테스트검색어");
            recent.record("테스트검색어", List.of(article()));
            RecentSearchesPort.Turn again = recent.take("테스트검색어");

            assertThat(turn.reused()).isEmpty();
            assertThat(turn.owner()).isFalse();
            assertThat(again.reused()).isEmpty();
            assertThat(recent.claim("테스트검색어")).isFalse();
            // 첫 실패 뒤에는 Redis 를 부르지 않아 대기 상한(5초)이나 제한 시간에 묶이지 않는다.
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(3));
        } finally {
            dead.destroy();
        }
    }
}

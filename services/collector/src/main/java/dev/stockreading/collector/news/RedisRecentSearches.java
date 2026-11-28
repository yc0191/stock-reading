package dev.stockreading.collector.news;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

/**
 * Redis 에 두는 최근 검색 결과 보관소(ADR-008). 수집기를 재시작해도 다시 쓸 수 있는 기간 안의 결과가 남는다.
 * 검색 중 표시는 만료가 있는 잠금이라, 표시를 잡은 쪽이 죽어도 lockTtl 뒤에는 저절로 풀린다.
 * 잃어도 되는 캐시라 Redis 가 닿지 않으면 비어 있는 것처럼 굴어 부르는 쪽이 그냥 검색하게 한다.
 * 정기 수집(스케줄러 스레드)과 재검색(Kafka 리스너 스레드)이 함께 부르므로 스레드 사이에서 안전해야 한다.
 */
@Component
class RedisRecentSearches implements RecentSearchesPort {

    private static final Logger log = LoggerFactory.getLogger(RedisRecentSearches.class);

    // 검색어는 관심 종목이 드러날 수 있어 키에는 해시만 쓴다.
    static final String RESULT_PREFIX = "collector:recent-search:";
    static final String LOCK_PREFIX = "collector:recent-search-lock:";

    // 한 번 실패하면 이 시간 동안 Redis 를 부르지 않는다. 부를 때마다 제한 시간만큼 묶이면 검색어가 많은 수집이 길어진다.
    static final Duration PAUSE_AFTER_FAILURE = Duration.ofSeconds(30);

    // 잠금 값이 내 것일 때만 지운다. 만료로 풀린 뒤 다른 쪽이 잡은 잠금을 지우지 않기 위해서다.
    private static final RedisScript<Long> DELETE_IF_OWNED = RedisScript.of(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private record Stored(long searchedAtMillis, List<StoredItem> items) {
    }

    // 발행 시각의 오프셋과 종목·검색어 순서를 그대로 되살리려고 문자열과 목록으로 담는다.
    private record StoredItem(String title, String summary, String url, String providerUrl, String publishedAt,
            String source, List<String> symbols, List<String> matchedQueries) {

        static StoredItem of(NewsItem item) {
            return new StoredItem(item.title(), item.summary(), item.url(), item.providerUrl(),
                    item.publishedAt().toString(), item.source(), List.copyOf(item.symbols()),
                    List.copyOf(item.matchedQueries()));
        }

        NewsItem toItem() {
            return new NewsItem(title, summary, url, providerUrl, OffsetDateTime.parse(publishedAt), source,
                    new LinkedHashSet<>(symbols), new LinkedHashSet<>(matchedQueries));
        }
    }

    private final StringRedisTemplate redis;
    private final Clock clock;
    private final RecentSearchProperties properties;
    // 이 프로세스가 잡은 잠금의 값. record·release 가 자기 잠금만 풀게 한다.
    private final Map<String, String> ownedLocks = new ConcurrentHashMap<>();
    // 시작할 때는 멈춰 있지 않다. nanoTime 은 차이로만 비교해야 넘침에 안전하다.
    private final AtomicLong pausedUntilNanos = new AtomicLong(System.nanoTime());
    private volatile boolean down;

    RedisRecentSearches(StringRedisTemplate redis, Clock clock, RecentSearchProperties properties) {
        this.redis = redis;
        this.clock = clock;
        this.properties = properties;
    }

    @Override
    public Turn take(String key) {
        // 대기 상한은 실제로 흐른 시간으로 잰다. 주입한 시계는 테스트에서 멈춰 있을 수 있다.
        long deadline = System.nanoTime() + properties.waitLimit().toNanos();
        while (true) {
            Optional<List<NewsItem>> recent = find(key);
            if (recent.isPresent()) {
                return Turn.reuse(recent.get());
            }
            if (claim(key)) {
                // 확인한 뒤 표시를 잡기 전에 다른 쪽이 검색을 끝냈을 수 있다.
                Optional<List<NewsItem>> justRecorded = find(key);
                if (justRecorded.isPresent()) {
                    release(key);
                    return Turn.reuse(justRecorded.get());
                }
                return Turn.search(true);
            }
            if (isPaused()) {
                return Turn.search(false);
            }
            // 다른 쪽이 검색 중이다. 결과가 남거나 표시가 풀릴 때까지 짧은 간격으로 다시 읽는다.
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return Turn.search(false);
                }
                try {
                    Thread.sleep(Duration.ofNanos(Math.min(remaining, properties.pollInterval().toNanos())));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Turn.search(false);
                }
                Optional<List<NewsItem>> waited = find(key);
                if (waited.isPresent()) {
                    return Turn.reuse(waited.get());
                }
                Boolean locked = call(() -> redis.hasKey(LOCK_PREFIX + hash(key)), null);
                if (locked == null) {
                    return Turn.search(false);
                }
                if (!locked) {
                    // 앞 검색이 결과 없이 끝났다(실패). 깨어난 쪽끼리 다시 정해 한 곳만 검색하게 한다.
                    break;
                }
            }
        }
    }

    @Override
    public boolean claim(String key) {
        String token = UUID.randomUUID().toString();
        Boolean set = call(() -> redis.opsForValue().setIfAbsent(LOCK_PREFIX + hash(key), token, properties.lockTtl()),
                null);
        if (Boolean.TRUE.equals(set)) {
            ownedLocks.put(key, token);
            return true;
        }
        return false;
    }

    @Override
    public void record(String key, List<NewsItem> found) {
        Stored stored = new Stored(clock.millis(), found.stream().map(StoredItem::of).toList());
        String value = JSON.writeValueAsString(stored);
        call(() -> {
            redis.opsForValue().set(RESULT_PREFIX + hash(key), value, properties.window());
            return null;
        }, null);
        release(key);
    }

    @Override
    public void release(String key) {
        String token = ownedLocks.remove(key);
        if (token != null) {
            call(() -> redis.execute(DELETE_IF_OWNED, List.of(LOCK_PREFIX + hash(key)), token), null);
        }
    }

    // 다시 쓸 수 있는 기간 안의 결과만 돌려준다. 기간은 수집기 시계로 판단하고 Redis 만료는 지난 키를 치우는 데만 쓴다.
    Optional<List<NewsItem>> find(String key) {
        String value = call(() -> redis.opsForValue().get(RESULT_PREFIX + hash(key)), null);
        if (value == null) {
            return Optional.empty();
        }
        Stored stored;
        try {
            stored = JSON.readValue(value, Stored.class);
        } catch (RuntimeException e) {
            log.warn("recent search entry unreadable, searching instead errorType={}", e.getClass().getSimpleName());
            return Optional.empty();
        }
        Instant searchedAt = Instant.ofEpochMilli(stored.searchedAtMillis());
        if (!clock.instant().isBefore(searchedAt.plus(properties.window()))) {
            return Optional.empty();
        }
        return Optional.of(stored.items().stream().map(StoredItem::toItem).toList());
    }

    static String hash(String key) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private boolean isPaused() {
        return System.nanoTime() - pausedUntilNanos.get() < 0;
    }

    // Redis 호출이 실패하면 경고를 한 번만 남기고 fallback 을 돌려준다. 다시 닿으면 한 번 알린다.
    private <T> T call(RedisCall<T> action, T fallback) {
        if (isPaused()) {
            return fallback;
        }
        try {
            T result = action.run();
            if (down) {
                down = false;
                log.info("redis reachable again, recent search reuse resumed");
            }
            return result;
        } catch (RuntimeException e) {
            pausedUntilNanos.set(System.nanoTime() + PAUSE_AFTER_FAILURE.toNanos());
            if (!down) {
                down = true;
                log.warn("redis unavailable, searching without reuse for {}s errorType={}",
                        PAUSE_AFTER_FAILURE.toSeconds(), e.getClass().getSimpleName());
            }
            return fallback;
        }
    }

    @FunctionalInterface
    private interface RedisCall<T> {
        T run();
    }
}

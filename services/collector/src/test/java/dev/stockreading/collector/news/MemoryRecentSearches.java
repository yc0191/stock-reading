package dev.stockreading.collector.news;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 테스트용 최근 검색 결과 보관소. 운영 구현(RedisRecentSearches)과 같은 약속(RecentSearchesPort)을 메모리로 지켜,
 * 수집 흐름 테스트가 Docker 없이 돌게 한다. 운영 구현의 동작은 RedisRecentSearchesTest 가 진짜 Redis 로 확인한다.
 * 정기 수집과 재검색을 여러 스레드에서 함께 부르는 테스트가 있어 스레드 사이에서 안전해야 한다.
 */
class MemoryRecentSearches implements RecentSearchesPort {

    private record Entry(Instant searchedAt, List<NewsItem> items) {
    }

    private final Clock clock;
    private final Duration window;
    private final Duration waitLimit;
    private final Map<String, Entry> results = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<List<NewsItem>>> searching = new ConcurrentHashMap<>();

    MemoryRecentSearches(Clock clock, Duration window, Duration waitLimit) {
        this.clock = clock;
        this.window = window;
        this.waitLimit = waitLimit;
    }

    @Override
    public Turn take(String key) {
        // 대기 상한은 실제로 흐른 시간으로 잰다. 주입한 시계는 테스트에서 멈춰 있을 수 있다.
        long deadline = System.nanoTime() + waitLimit.toNanos();
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
            CompletableFuture<List<NewsItem>> pending = searching.get(key);
            if (pending == null) {
                // 표시를 확인하는 사이 검색이 끝났다. 처음부터 다시 본다.
                continue;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return Turn.search(false);
            }
            try {
                return Turn.reuse(pending.get(remaining, TimeUnit.NANOSECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Turn.search(false);
            } catch (TimeoutException e) {
                return Turn.search(false);
            } catch (ExecutionException e) {
                // 앞 검색이 실패했다. 깨어난 쪽끼리 다시 정해 한 곳만 검색하게 한다.
            }
        }
    }

    @Override
    public boolean claim(String key) {
        return searching.putIfAbsent(key, new CompletableFuture<>()) == null;
    }

    @Override
    public void record(String key, List<NewsItem> found) {
        List<NewsItem> copy = List.copyOf(found);
        results.put(key, new Entry(clock.instant(), copy));
        CompletableFuture<List<NewsItem>> pending = searching.remove(key);
        if (pending != null) {
            pending.complete(copy);
        }
    }

    @Override
    public void release(String key) {
        CompletableFuture<List<NewsItem>> pending = searching.remove(key);
        if (pending != null) {
            pending.completeExceptionally(new IllegalStateException("search was not completed"));
        }
    }

    // 다시 쓸 수 있는 기간 안의 결과만 돌려준다.
    Optional<List<NewsItem>> find(String key) {
        Entry entry = results.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        if (!isFresh(entry, clock.instant())) {
            results.remove(key, entry);
            return Optional.empty();
        }
        return Optional.of(entry.items());
    }

    private boolean isFresh(Entry entry, Instant now) {
        return now.isBefore(entry.searchedAt().plus(window));
    }
}

package dev.stockreading.collector.news;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.stereotype.Component;

/**
 * 수집기 메모리에 두는 최근 검색 결과 보관소. 수집기를 재시작하면 비므로 그 직후 한 번은 다시 검색한다.
 * 정기 수집(스케줄러 스레드)과 재검색(Kafka 리스너 스레드)이 함께 부르므로 스레드 사이에서 안전해야 한다.
 */
@Component
class MemoryRecentSearches implements RecentSearchesPort {

    private record Entry(Instant searchedAt, List<NewsItem> items) {
    }

    private final Clock clock;
    private final RecentSearchProperties properties;
    private final Map<String, Entry> results = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<List<NewsItem>>> searching = new ConcurrentHashMap<>();

    MemoryRecentSearches(Clock clock, RecentSearchProperties properties) {
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
        trim();
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
        return now.isBefore(entry.searchedAt().plus(properties.window()));
    }

    // 지난 결과를 먼저 지우고, 그래도 상한을 넘으면 오래된 검색어부터 지운다.
    private void trim() {
        if (results.size() <= properties.maxEntries()) {
            return;
        }
        Instant now = clock.instant();
        results.entrySet().removeIf(e -> !isFresh(e.getValue(), now));
        int over = results.size() - properties.maxEntries();
        if (over > 0) {
            results.entrySet().stream()
                    .sorted(Comparator.comparing(e -> e.getValue().searchedAt()))
                    .limit(over)
                    .map(Map.Entry::getKey)
                    .toList()
                    .forEach(results::remove);
        }
    }
}

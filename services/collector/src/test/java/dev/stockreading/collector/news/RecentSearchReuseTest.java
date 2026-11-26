package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/** #98 최근 검색 다시 쓰기의 끝 기준을 가짜 검색 제공자로 확인한다. */
class RecentSearchReuseTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final Instant START = Instant.parse("2026-10-22T07:10:00Z");

    private final NewsSearchPort search = mock(NewsSearchPort.class);
    private final MutableClock clock = new MutableClock(START);

    private NewsCollector collector(Duration waitLimit) {
        MemoryRecentSearches recent = new MemoryRecentSearches(clock,
                new RecentSearchProperties(Duration.ofMinutes(15), waitLimit, 100));
        return new NewsCollector(search, new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, 100, 0),
                clock, ms -> { }, () -> List.of(), recent);
    }

    private static NewsItem article(String query, String url) {
        return new NewsItem("기사 " + url, query + " 요약", url, "", OffsetDateTime.parse("2026-10-22T15:00:00+09:00"),
                "example.com", new LinkedHashSet<>(), new LinkedHashSet<>(Set.of(query)));
    }

    private static NewsQuery query(String q, String symbol) {
        return new NewsQuery(q, new LinkedHashSet<>(List.of(symbol)));
    }

    @Test
    void sameQueryTwiceWithinTheWindowCallsTheProviderOnceAndBothGetTheSameArticles() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        NewsCollector collector = collector(Duration.ZERO);

        NewsCollection first = collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));
        clock.advance(Duration.ofMinutes(14).plusSeconds(59));
        NewsCollection second = collector.researchWithStatus(List.of(query(" 테스트검색어 ", "TEST02")));

        verify(search, times(1)).search(anyString(), anyInt());
        assertThat(first.items()).extracting(NewsItem::url).containsExactly("https://a.example.com/1");
        assertThat(second.items()).extracting(NewsItem::url).containsExactly("https://a.example.com/1");
        // 다시 쓴 결과에도 이번 요청의 종목 태그를 붙인다.
        assertThat(second.items().get(0).symbols()).containsExactly("TEST02");
        assertThat(second.incompleteSymbols()).isEmpty();
    }

    @Test
    void sameQueryAfterTheWindowIsSearchedAgain() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        NewsCollector collector = collector(Duration.ZERO);

        collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));
        clock.advance(Duration.ofMinutes(15));
        collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));

        verify(search, times(2)).search(anyString(), anyInt());
    }

    @Test
    void researchReusesWhatTheRegularCollectionJustSearched() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        NewsCollector collector = collector(Duration.ZERO);

        collector.collectWithStatus(List.of(query("테스트검색어", "TEST01")));
        clock.advance(Duration.ofMinutes(5));
        NewsCollection research = collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));

        verify(search, times(1)).search(anyString(), anyInt());
        assertThat(research.items()).hasSize(1);
    }

    @Test
    void regularCollectionAlwaysSearchesEvenRightAfterAResearch() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        NewsCollector collector = collector(Duration.ZERO);

        collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));
        collector.collectWithStatus(List.of(query("테스트검색어", "TEST01")));

        verify(search, times(2)).search(anyString(), anyInt());
    }

    @Test
    void reusedResultIsFilteredAgainAtTheTimeOfReuse() {
        // 15:00 기사는 처음 검색 때는 24시간 창 안이지만, 다시 쓰는 시각을 넘겨 창 밖이 되면 빠져야 한다.
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        MutableClock late = new MutableClock(OffsetDateTime.parse("2026-10-23T14:55:00+09:00").toInstant());
        NewsCollector collector = new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, 100, 0), late, ms -> { }, () -> List.of(),
                new MemoryRecentSearches(late, new RecentSearchProperties(Duration.ofMinutes(15), Duration.ZERO, 100)));

        assertThat(collector.researchWithStatus(List.of(query("테스트검색어", "TEST01"))).items()).hasSize(1);
        late.advance(Duration.ofMinutes(10));
        assertThat(collector.researchWithStatus(List.of(query("테스트검색어", "TEST01"))).items()).isEmpty();

        verify(search, times(1)).search(anyString(), anyInt());
    }

    @Test
    void failedSearchIsNotRememberedSoTheNextRequestSearchesAgain() {
        when(search.search(eq("테스트검색어"), anyInt()))
                .thenThrow(new NewsSearchApiException("boom"))
                .thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        NewsCollector collector = collector(Duration.ZERO);

        NewsCollection failed = collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));
        NewsCollection retried = collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));

        assertThat(failed.incompleteSymbols()).containsExactly("TEST01");
        assertThat(retried.items()).hasSize(1);
        verify(search, times(2)).search(anyString(), anyInt());
    }

    @Test
    void secondRequestWaitsForTheSearchInProgressInsteadOfCallingAgain() throws Exception {
        CountDownLatch searching = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        when(search.search(eq("테스트검색어"), anyInt())).thenAnswer(invocation -> {
            searching.countDown();
            finish.await(5, TimeUnit.SECONDS);
            return List.of(article("테스트검색어", "https://a.example.com/1"));
        });
        NewsCollector collector = collector(Duration.ofSeconds(5));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<NewsCollection> regular = pool.submit(() -> collector.collectWithStatus(List.of(query("테스트검색어", "TEST01"))));
            assertThat(searching.await(5, TimeUnit.SECONDS)).isTrue();
            Future<NewsCollection> research = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST02"))));
            finish.countDown();

            assertThat(regular.get(5, TimeUnit.SECONDS).items()).hasSize(1);
            assertThat(research.get(5, TimeUnit.SECONDS).items()).extracting(NewsItem::url)
                    .containsExactly("https://a.example.com/1");
            verify(search, times(1)).search(anyString(), anyInt());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waiterSearchesItselfWhenTheSearchInProgressFails() throws Exception {
        CountDownLatch searching = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        when(search.search(eq("테스트검색어"), anyInt()))
                .thenAnswer(invocation -> {
                    searching.countDown();
                    finish.await(5, TimeUnit.SECONDS);
                    throw new NewsSearchApiException("boom");
                })
                .thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        NewsCollector collector = collector(Duration.ofSeconds(5));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<NewsCollection> first = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST01"))));
            assertThat(searching.await(5, TimeUnit.SECONDS)).isTrue();
            Future<NewsCollection> second = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST02"))));
            finish.countDown();

            assertThat(first.get(5, TimeUnit.SECONDS).incompleteSymbols()).containsExactly("TEST01");
            assertThat(second.get(5, TimeUnit.SECONDS).items()).hasSize(1);
            verify(search, times(2)).search(anyString(), anyInt());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void unexpectedSearchErrorStillFreesTheQueryForTheNextSearch() {
        when(search.search(eq("테스트검색어"), anyInt()))
                .thenThrow(new IllegalStateException("응답 해석 실패"))
                .thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        MemoryRecentSearches recent = new MemoryRecentSearches(clock,
                new RecentSearchProperties(Duration.ofMinutes(15), Duration.ofSeconds(5), 100));
        NewsCollector collector = new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, 100, 0), clock, ms -> { }, () -> List.of(), recent);

        assertThatThrownBy(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST01"))))
                .isInstanceOf(IllegalStateException.class);

        // 표시가 남았다면 다음 요청이 대기 상한(5초)까지 묶인 뒤에야 검색한다.
        long started = System.nanoTime();
        assertThat(collector.researchWithStatus(List.of(query("테스트검색어", "TEST01"))).items()).hasSize(1);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(1));
    }

    @Test
    void resultRecordedJustBeforeTheClaimIsReusedInsteadOfSearchingAgain() {
        List<NewsItem> other = List.of(article("테스트검색어", "https://a.example.com/1"));
        // 확인과 표시 사이에 다른 쪽이 검색을 끝낸 상황을 만든다.
        MemoryRecentSearches recent = new MemoryRecentSearches(clock,
                new RecentSearchProperties(Duration.ofMinutes(15), Duration.ZERO, 100)) {
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

        RecentSearchesPort.Turn turn = recent.take("테스트검색어");

        assertThat(turn.reused()).contains(other);
        assertThat(turn.owner()).isFalse();
        // 잡았던 표시는 풀어 둔다.
        assertThat(recent.claim("테스트검색어")).isTrue();
    }

    @Test
    void waitersWokenByAFailedSearchLetOnlyOneOfThemSearchAgain() throws Exception {
        CountDownLatch searching = new CountDownLatch(1);
        CountDownLatch failFirst = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        when(search.search(eq("테스트검색어"), anyInt())).thenAnswer(invocation -> {
            if (calls.incrementAndGet() == 1) {
                searching.countDown();
                failFirst.await(5, TimeUnit.SECONDS);
                throw new NewsSearchApiException("boom");
            }
            Thread.sleep(200);
            return List.of(article("테스트검색어", "https://a.example.com/1"));
        });
        NewsCollector collector = collector(Duration.ofSeconds(5));
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            Future<NewsCollection> first = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST01"))));
            assertThat(searching.await(5, TimeUnit.SECONDS)).isTrue();
            Future<NewsCollection> second = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST02"))));
            Future<NewsCollection> third = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST03"))));
            // 두 요청이 앞 검색을 기다리기 시작할 시간을 준다.
            Thread.sleep(300);
            failFirst.countDown();

            assertThat(first.get(5, TimeUnit.SECONDS).incompleteSymbols()).containsExactly("TEST01");
            assertThat(second.get(5, TimeUnit.SECONDS).items()).hasSize(1);
            assertThat(third.get(5, TimeUnit.SECONDS).items()).hasSize(1);
            assertThat(calls.get()).isEqualTo(2);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void waiterGivesUpAtTheWaitLimitAndSearchesItself() throws Exception {
        CountDownLatch searching = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        when(search.search(eq("테스트검색어"), anyInt())).thenAnswer(invocation -> {
            if (calls.incrementAndGet() == 1) {
                searching.countDown();
                finish.await(5, TimeUnit.SECONDS);
            }
            return List.of(article("테스트검색어", "https://a.example.com/1"));
        });
        NewsCollector collector = collector(Duration.ofMillis(200));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<NewsCollection> slow = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST01"))));
            assertThat(searching.await(5, TimeUnit.SECONDS)).isTrue();

            NewsCollection waited = pool.submit(() -> collector.researchWithStatus(List.of(query("테스트검색어", "TEST02"))))
                    .get(5, TimeUnit.SECONDS);
            finish.countDown();

            assertThat(waited.items()).hasSize(1);
            assertThat(slow.get(5, TimeUnit.SECONDS).items()).hasSize(1);
            assertThat(calls.get()).isEqualTo(2);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void reusedQueriesDoNotSpendTheDailyCallLimit() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(article("테스트검색어", "https://a.example.com/1")));
        NewsCollector collector = new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, 1, 0), clock, ms -> { }, () -> List.of(),
                new MemoryRecentSearches(clock, new RecentSearchProperties(Duration.ofMinutes(15), Duration.ZERO, 100)));

        collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));
        NewsCollection again = collector.researchWithStatus(List.of(query("테스트검색어", "TEST01")));

        assertThat(again.items()).hasSize(1);
        assertThat(again.incompleteSymbols()).isEmpty();
    }

    @Test
    void keyIgnoresSurroundingAndRepeatedSpacesButKeepsCase() {
        assertThat(RecentSearchesPort.keyOf("  테스트   검색어 ")).isEqualTo("테스트 검색어");
        assertThat(RecentSearchesPort.keyOf("ETF")).isNotEqualTo(RecentSearchesPort.keyOf("etf"));
    }

    @Test
    void oldestQueriesAreDroppedOverTheEntryLimit() {
        MemoryRecentSearches recent = new MemoryRecentSearches(clock,
                new RecentSearchProperties(Duration.ofMinutes(15), Duration.ZERO, 2));
        recent.record("가", List.of());
        clock.advance(Duration.ofSeconds(1));
        recent.record("나", List.of());
        clock.advance(Duration.ofSeconds(1));
        recent.record("다", List.of());

        assertThat(recent.find("가")).isEmpty();
        assertThat(recent.find("나")).isPresent();
        assertThat(recent.find("다")).isPresent();
    }

    private static final class MutableClock extends Clock {
        private volatile Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return KST;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}

package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class NewsCollectorTest {

    // 아래 기사 시각(2026-10-22 10:00~16:08 KST)이 모두 24시간 창 안에 들어오도록 맞춘 수집 시각.
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T07:10:00Z"), ZoneId.of("Asia/Seoul"));

    private final NewsSearchPort search = mock(NewsSearchPort.class);
    private final NewsCollector collector =
            new NewsCollector(search, new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", 100, 0), CLOCK, ms -> { });

    private static NewsItem item(String query, String title, String url, String time) {
        // 요약에 검색어를 넣어 검색어 토큰 필터를 통과하게 한다.
        return new NewsItem(title, query + " 요약", url, "", OffsetDateTime.parse(time), "example.com",
                new LinkedHashSet<>(), new LinkedHashSet<>(Set.of(query)));
    }

    private static NewsQuery query(String q, String... symbols) {
        return new NewsQuery(q, new LinkedHashSet<>(List.of(symbols)));
    }

    @Test
    void sameUrlFromTwoQueriesIsMergedWithUnionOfSymbolsAndQueriesAndTheEarlierTime() {
        when(search.search(eq("테스트검색어"), anyInt()))
                .thenReturn(List.of(item("테스트검색어", "시황 급락", "https://a.example.com/1?utm_source=x", "2026-10-22T16:08:00+09:00")));
        when(search.search(eq("테스트시황"), anyInt()))
                .thenReturn(List.of(item("테스트시황", "시황 급락", "http://A.example.com/1/", "2026-10-22T16:07:00+09:00")));

        List<NewsItem> result = collector.collect(List.of(query("테스트검색어", "TEST01"), query("테스트시황", "*")));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).symbols()).containsExactlyInAnyOrder("TEST01", "*");
        assertThat(result.get(0).matchedQueries()).containsExactlyInAnyOrder("테스트검색어", "테스트시황");
        assertThat(result.get(0).publishedAt()).isEqualTo(OffsetDateTime.parse("2026-10-22T16:07:00+09:00"));
    }

    @Test
    void sameCleanedTitleOnDifferentUrlsIsMerged() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(
                item("테스트검색어", "[특징주] 테스트종목, 4% 하락", "https://a.example.com/1", "2026-10-22T16:08:00+09:00"),
                item("테스트검색어", "테스트종목 4% 하락", "https://b.example.com/9", "2026-10-22T16:06:00+09:00")));

        List<NewsItem> result = collector.collect(List.of(query("테스트검색어", "TEST01")));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).url()).isEqualTo("https://a.example.com/1");
    }

    @Test
    void differentArticlesStayAndAreSortedNewestFirst() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(
                item("테스트검색어", "기사 하나", "https://a.example.com/1", "2026-10-22T10:00:00+09:00"),
                item("테스트검색어", "기사 둘", "https://a.example.com/2", "2026-10-22T12:00:00+09:00")));

        List<NewsItem> result = collector.collect(List.of(query("테스트검색어", "TEST01")));

        assertThat(result).extracting(NewsItem::title).containsExactly("기사 둘", "기사 하나");
    }

    @Test
    void failedQueryIsSkippedAndTheRestContinue() {
        when(search.search(eq("실패"), anyInt())).thenThrow(new NewsSearchApiException("boom"));
        when(search.search(eq("테스트검색어"), anyInt()))
                .thenReturn(List.of(item("테스트검색어", "기사", "https://a.example.com/1", "2026-10-22T10:00:00+09:00")));

        List<NewsItem> result = collector.collect(List.of(query("실패", "X"), query("테스트검색어", "TEST01")));

        assertThat(result).extracting(NewsItem::title).containsExactly("기사");
    }

    @Test
    void punctuationOnlyTitlesAreNotMergedByTitle() {
        when(search.search(eq("q"), anyInt())).thenReturn(List.of(
                item("q", "...", "https://a.example.com/1", "2026-10-22T10:00:00+09:00"),
                item("q", "???", "https://a.example.com/2", "2026-10-22T11:00:00+09:00")));

        assertThat(collector.collect(List.of(query("q", "A")))).hasSize(2);
    }

    @Test
    void itemsFailingTheFilterAreDroppedBeforeAnythingElse() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(
                item("테스트검색어", "정상 기사", "https://a.example.com/1", "2026-10-22T15:00:00+09:00"),
                item("테스트검색어", "이틀 지난 기사", "https://a.example.com/2", "2026-10-20T15:00:00+09:00"),
                item("테스트검색어", "먼 미래 기사", "https://a.example.com/3", "2026-10-22T20:00:00+09:00"),
                new NewsItem("무관한 기사", "다른 내용", "https://a.example.com/4", "",
                        OffsetDateTime.parse("2026-10-22T15:00:00+09:00"), "example.com",
                        new LinkedHashSet<>(), new LinkedHashSet<>(Set.of("테스트검색어")))));

        List<NewsItem> result = collector.collect(List.of(query("테스트검색어", "TEST01")));

        assertThat(result).extracting(NewsItem::title).containsExactly("정상 기사");
    }

    @Test
    void filterRunsPerQueryBeforeMergeSoAnUnmatchedQueryDoesNotTagTheArticle() {
        NewsItem article = item("테스트검색어", "시황 기사", "https://a.example.com/1", "2026-10-22T15:00:00+09:00");
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(article));
        // 같은 기사가 다른 검색어로도 왔지만 그 검색어 토큰은 제목·요약에 없다.
        when(search.search(eq("무관한검색어"), anyInt())).thenReturn(List.of(article));

        List<NewsItem> result = collector.collect(List.of(query("테스트검색어", "TEST01"), query("무관한검색어", "TEST02")));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).symbols()).containsExactly("TEST01");
    }

    @Test
    void queriesBeyondTheDailyCallLimitAreSkippedWithoutCallingTheApi() {
        NewsCollector limited = new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", 2, 0), CLOCK, ms -> { });
        when(search.search(anyString(), anyInt())).thenReturn(List.of());

        limited.collect(List.of(query("가", "A"), query("나", "A"), query("다", "A")));
        limited.collect(List.of(query("라", "A")));

        verify(search, times(2)).search(anyString(), anyInt());
        verify(search, never()).search(eq("다"), anyInt());
        verify(search, never()).search(eq("라"), anyInt());
    }

    @Test
    void failedCallsStillCountTowardTheDailyLimit() {
        NewsCollector limited = new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", 1, 0), CLOCK, ms -> { });
        when(search.search(eq("가"), anyInt())).thenThrow(new NewsSearchApiException("boom"));

        limited.collect(List.of(query("가", "A"), query("나", "A")));

        verify(search, never()).search(eq("나"), anyInt());
    }

    private NewsCollector collectorWithInterval(long intervalMs, int dailyLimit, List<Long> pauses) {
        return new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", dailyLimit, intervalMs), CLOCK, pauses::add);
    }

    @Test
    void pausesBetweenConsecutiveCallsButNotBeforeTheFirst() {
        List<Long> pauses = new ArrayList<>();
        when(search.search(anyString(), anyInt())).thenReturn(List.of());

        collectorWithInterval(200, 100, pauses).collect(List.of(query("가", "A"), query("나", "A"), query("다", "A")));

        assertThat(pauses).containsExactly(200L, 200L);
    }

    @Test
    void doesNotPauseWhenTheIntervalIsZero() {
        List<Long> pauses = new ArrayList<>();
        when(search.search(anyString(), anyInt())).thenReturn(List.of());

        collectorWithInterval(0, 100, pauses).collect(List.of(query("가", "A"), query("나", "A")));

        assertThat(pauses).isEmpty();
    }

    @Test
    void failedCallsAreFollowedByAPauseToo() {
        List<Long> pauses = new ArrayList<>();
        when(search.search(eq("가"), anyInt())).thenThrow(new NewsSearchApiException("boom"));
        when(search.search(eq("나"), anyInt())).thenReturn(List.of());

        collectorWithInterval(100, 100, pauses).collect(List.of(query("가", "A"), query("나", "A")));

        assertThat(pauses).containsExactly(100L);
    }

    @Test
    void skippedQueriesOverTheDailyLimitDoNotPause() {
        List<Long> pauses = new ArrayList<>();
        when(search.search(anyString(), anyInt())).thenReturn(List.of());

        collectorWithInterval(100, 1, pauses).collect(List.of(query("가", "A"), query("나", "A"), query("다", "A")));

        assertThat(pauses).isEmpty();
    }

    @Test
    void negativeIntervalIsRejectedAtStartup() {
        assertThatThrownBy(() -> new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", 1, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sameArticleWithAndWithoutWwwIsMergedByUrl() {
        // 제목을 다르게 해서 URL 키만으로 합쳐지는지 확인한다.
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(
                item("테스트검색어", "제목 하나", "https://www.a.example.com/1", "2026-10-22T15:00:00+09:00"),
                item("테스트검색어", "전혀 다른 제목", "https://a.example.com/1", "2026-10-22T14:00:00+09:00")));

        List<NewsItem> result = collector.collect(List.of(query("테스트검색어", "TEST01")));

        assertThat(result).hasSize(1);
    }

    private NewsCollector collectorWithQueries(String queries) {
        return new NewsCollector(search, new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, queries, 100, 0), CLOCK, ms -> { });
    }

    @Test
    void malformedSymbolInTheQueryConfigurationFailsAtConstruction() {
        assertThatThrownBy(() -> collectorWithQueries("테스트검색어=TEST1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("테스트검색어");
    }

    @Test
    void emptyQueryConfigurationIsNotAnErrorAndCollectsNothing() {
        assertThat(collectorWithQueries("").collect()).isEmpty();
        verify(search, never()).search(anyString(), anyInt());
    }

    @Test
    void collectWithoutArgumentsUsesTheConfiguredQueriesIncludingMarketWide() {
        when(search.search(eq("테스트검색어"), anyInt()))
                .thenReturn(List.of(item("테스트검색어", "기사", "https://a.example.com/1", "2026-10-22T15:00:00+09:00")));

        List<NewsItem> result = collectorWithQueries("테스트검색어=TEST01,TEST02;테스트시황=*").collect();

        assertThat(result).hasSize(1);
        assertThat(result.get(0).symbols()).containsExactly("TEST01", "TEST02");
        verify(search).search(eq("테스트시황"), anyInt());
    }
}

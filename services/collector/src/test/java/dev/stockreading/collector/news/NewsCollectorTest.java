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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

class NewsCollectorTest {

    // 아래 기사 시각(2026-10-22 10:00~16:08 KST)이 모두 24시간 창 안에 들어오도록 맞춘 수집 시각.
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T07:10:00Z"), ZoneId.of("Asia/Seoul"));

    private final NewsSearchPort search = mock(NewsSearchPort.class);
    private final NewsCollector collector =
            new NewsCollector(search, new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", 100, 0), CLOCK, ms -> { });

    private ListAppender<ILoggingEvent> logs;

    @BeforeEach
    void attachLogs() {
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(NewsCollector.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(NewsCollector.class)).detachAppender(logs);
    }

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

    @Test
    void failedQuerySymbolsAreReportedAsIncomplete() {
        when(search.search(eq("실패"), anyInt())).thenThrow(new NewsSearchApiException("boom"));
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of());

        NewsCollection result = collector.collectWithStatus(
                List.of(query("실패", "TEST02", "TEST03"), query("테스트검색어", "TEST01")));

        assertThat(result.incompleteSymbols()).containsExactlyInAnyOrder("TEST02", "TEST03");
    }

    @Test
    void failedMarketWideQueryReportsTheMarketWideSymbol() {
        when(search.search(eq("테스트시황"), anyInt())).thenThrow(new NewsSearchApiException("boom"));

        NewsCollection result = collector.collectWithStatus(List.of(query("테스트시황", "*")));

        assertThat(result.incompleteSymbols()).containsExactly("*");
    }

    @Test
    void symbolsOfQueriesSkippedByTheDailyLimitAreReportedAsIncomplete() {
        NewsCollector limited = new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", 1, 0), CLOCK, ms -> { });
        when(search.search(anyString(), anyInt())).thenReturn(List.of());

        NewsCollection result = limited.collectWithStatus(
                List.of(query("가", "TEST01"), query("나", "TEST02"), query("다", "*")));

        assertThat(result.incompleteSymbols()).containsExactlyInAnyOrder("TEST02", "*");
    }

    @Test
    void fullyCollectedRunHasNoIncompleteSymbols() {
        when(search.search(anyString(), anyInt())).thenReturn(List.of());

        NewsCollection result = collector.collectWithStatus(List.of(query("가", "TEST01")));

        assertThat(result.incompleteSymbols()).isEmpty();
    }

    @Test
    void startedAtIsWhenTheFirstSearchCallBegan() {
        List<OffsetDateTime> callTimes = new ArrayList<>();
        Instant[] now = {Instant.parse("2026-10-22T07:10:00Z")};
        Clock moving = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZoneId.of("Asia/Seoul");
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return this;
            }

            @Override
            public Instant instant() {
                return now[0];
            }
        };
        when(search.search(anyString(), anyInt())).thenAnswer(invocation -> {
            callTimes.add(OffsetDateTime.ofInstant(moving.instant(), ZoneId.of("Asia/Seoul")));
            now[0] = now[0].plusSeconds(30);
            return List.of();
        });
        NewsCollector timed = new NewsCollector(search,
                new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, "", 100, 0), moving, ms -> { });

        NewsCollection result = timed.collectWithStatus(List.of(query("가", "A"), query("나", "A")));

        assertThat(callTimes).hasSize(2);
        assertThat(result.startedAt()).isEqualTo(callTimes.get(0));
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

    // 재검색은 설정 검색어 대신 요청 검색어를 같은 collectWithStatus 로 넘긴다. 하루 상한은 정기 수집과 함께 센다.
    @Test
    void researchQueriesShareTheDailyLimitWithTheRegularCollection() {
        NewsCollector limited = collectorWithLimit("테스트검색어=TEST02", 2);
        when(search.search(anyString(), anyInt())).thenReturn(List.of());

        limited.collectWithStatus();
        NewsCollection research = limited.collectWithStatus(
                List.of(query("테스트 재검색어 하나", "TEST01"), query("테스트 재검색어 둘", "TEST01")));

        verify(search, times(2)).search(anyString(), anyInt());
        assertThat(research.incompleteSymbols()).containsExactly("TEST01");
    }

    @Test
    void researchQueriesUseTheSameFilterAsTheRegularCollection() {
        when(search.search(eq("테스트 재검색어"), anyInt())).thenReturn(List.of(
                item("테스트 재검색어", "어제보다 오래된 기사", "https://a.example.com/old", "2026-10-21T16:00:00+09:00"),
                new NewsItem("검색어가 없는 기사", "무관한 요약", "https://a.example.com/off", "",
                        OffsetDateTime.parse("2026-10-22T15:00:00+09:00"), "example.com",
                        new LinkedHashSet<>(), new LinkedHashSet<>(Set.of("테스트 재검색어"))),
                item("테스트 재검색어", "남는 기사", "https://a.example.com/ok", "2026-10-22T15:00:00+09:00")));

        NewsCollection research = collector.collectWithStatus(List.of(query("테스트 재검색어", "TEST01")));

        assertThat(research.items()).extracting(NewsItem::title).containsExactly("남는 기사");
        assertThat(research.items().get(0).symbols()).containsExactly("TEST01");
    }

    @Test
    void failedResearchQueryMarksTheRequestSymbolIncomplete() {
        when(search.search(anyString(), anyInt())).thenThrow(new NewsSearchApiException("뉴스 검색 오류 http=500 code="));

        NewsCollection research = collector.collectWithStatus(List.of(query("테스트 재검색어", "*")));

        assertThat(research.items()).isEmpty();
        assertThat(research.incompleteSymbols()).containsExactly("*");
    }

    // 수집 한 번마다 거른 건수를 이유별로 한 줄 남긴다. 검색어와 종목은 쓰지 않는다.
    @Test
    void oneCollectionLogsHowManyArticlesWereFilteredPerReasonWithoutQueriesOrSymbols() {
        when(search.search(eq("테스트검색어"), anyInt())).thenReturn(List.of(
                item("테스트검색어", "어제보다 오래된 기사", "https://a.example.com/old", "2026-10-21T06:00:00+09:00"),
                item("테스트검색어", "미래 기사", "https://a.example.com/future", "2026-10-22T20:00:00+09:00"),
                new NewsItem("검색어가 없는 기사", "무관한 요약", "https://a.example.com/off", "",
                        OffsetDateTime.parse("2026-10-22T15:00:00+09:00"), "example.com",
                        new LinkedHashSet<>(), new LinkedHashSet<>(Set.of("테스트검색어"))),
                item("테스트검색어", "남는 기사", "https://a.example.com/ok", "2026-10-22T15:00:00+09:00")));
        when(search.search(eq("테스트시황"), anyInt())).thenReturn(List.of(
                item("테스트시황", "남는 기사", "https://b.example.com/same-title", "2026-10-22T14:00:00+09:00"),
                item("테스트시황", "다른 기사", "https://a.example.com/ok", "2026-10-22T13:00:00+09:00")));

        collector.collect(List.of(query("테스트검색어", "TEST01"), query("테스트시황", "*")));

        List<String> filtered = logs.list.stream().map(ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("news filtered")).toList();
        assertThat(filtered).containsExactly("news filtered passed=3 tooOld=1 tooFarAhead=1 unmatched=1 duplicates=2");
        assertThat(logs.list).extracting(ILoggingEvent::getFormattedMessage)
                .noneMatch(m -> m.contains("테스트검색어") || m.contains("테스트시황") || m.contains("TEST01"));
    }

    private NewsCollector collectorWithLimit(String queries, int dailyLimit) {
        return new NewsCollector(search, new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, queries, dailyLimit, 0), CLOCK, ms -> { });
    }
}

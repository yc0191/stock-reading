package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class NewsCollectorTest {

    private final NewsSearchPort search = mock(NewsSearchPort.class);
    private final NewsCollector collector =
            new NewsCollector(search, new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, ""));

    private static NewsItem item(String query, String title, String url, String time) {
        return new NewsItem(title, "요약", url, "", OffsetDateTime.parse(time), "example.com",
                new LinkedHashSet<>(), new LinkedHashSet<>(Set.of(query)));
    }

    private static NewsQuery query(String q, String... symbols) {
        return new NewsQuery(q, new LinkedHashSet<>(List.of(symbols)));
    }

    @Test
    void sameUrlFromTwoQueriesIsMergedWithUnionOfSymbolsAndQueries() {
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
}

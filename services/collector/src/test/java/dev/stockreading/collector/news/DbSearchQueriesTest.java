package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.collector.news.DbSearchQueries.Row;

/** 가짜 값(TEST 접두 종목코드, 가짜 검색어)만 쓴다. DB에서 읽은 줄을 검색어 목록으로 바꾸는 규칙을 본다. */
class DbSearchQueriesTest {

    @Test
    void rowsOfTheSameQueryMergeIntoOneQueryWithAllItsTagsIncludingMarketWide() {
        List<NewsQuery> queries = DbSearchQueries.group(List.of(
                new Row("테스트 검색어", "TEST01"), new Row("테스트 검색어", "TEST02"), new Row("테스트 시황", "*")));

        assertThat(queries).extracting(NewsQuery::query).containsExactly("테스트 검색어", "테스트 시황");
        assertThat(queries.get(0).symbols()).containsExactly("TEST01", "TEST02");
        assertThat(queries.get(1).symbols()).containsExactly("*");
    }

    @Test
    void usTickerTagsAreKeptAndOnlyTagsTheArticleEventCannotCarryAreDropped() {
        // 기사 이벤트의 종목 형식은 국내 6자리 코드, 미국 티커(종류 구분 "-"), "*"다. 그 밖의 형식은 기사가 통째로 버려지므로 읽을 때 거른다.
        List<NewsQuery> queries = DbSearchQueries.group(List.of(
                new Row("섞인 태그", "TEST01"), new Row("섞인 태그", "FAKE-B"), new Row("섞인 태그", "fake"),
                new Row("미국만", "FAKE"),
                new Row("  ", "TEST01"),
                new Row("형식 밖", "fake"), new Row("형식 밖", "TOOLONGX"),
                new Row("태그 없음", null)));

        assertThat(queries).extracting(NewsQuery::query).containsExactly("섞인 태그", "미국만");
        assertThat(queries.get(0).symbols()).containsExactly("TEST01", "FAKE-B");
        assertThat(queries.get(1).symbols()).containsExactly("FAKE");
    }

    @Test
    void noRowsMeansNoQueries() {
        assertThat(DbSearchQueries.group(List.of())).isEmpty();
    }
}

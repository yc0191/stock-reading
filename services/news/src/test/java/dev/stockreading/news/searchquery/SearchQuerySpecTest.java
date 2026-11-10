package dev.stockreading.news.searchquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.news.event.SearchQueriesPayload.Query;

/** 가짜 값(TEST 접두 종목코드, 가짜 검색어)만 쓴다. */
class SearchQuerySpecTest {

    @Test
    void parsesQueriesWithDomesticCodesUsTickersAndMarketWideAndNormalizesTheClassSeparator() {
        List<Query> queries = SearchQuerySpec.parse("테스트검색어=TEST01,FAKE.B; 테스트 시황 = * ;미국=FAKE-C");

        assertThat(queries).extracting(Query::query).containsExactly("테스트검색어", "테스트 시황", "미국");
        assertThat(queries.get(0).symbols()).containsExactly("TEST01", "FAKE-B");
        assertThat(queries.get(1).symbols()).containsExactly("*");
        assertThat(queries.get(2).symbols()).containsExactly("FAKE-C");
    }

    @Test
    void emptyBlankAndTrailingSeparatorsAreNotErrors() {
        assertThat(SearchQuerySpec.parse(null)).isEmpty();
        assertThat(SearchQuerySpec.parse("  \n ")).isEmpty();
        assertThat(SearchQuerySpec.parse("  ; ok=TEST01 ;; ok2=* ;  ;")).hasSize(2);
        assertThat(SearchQuerySpec.parse("ok=TEST01\nok2=*\n")).hasSize(2);
    }

    @Test
    void sameSymbolRepeatedInOneQueryIsKeptOnce() {
        assertThat(SearchQuerySpec.parse("ok=TEST01,TEST01,FAKE.B,FAKE-B").get(0).symbols())
                .containsExactly("TEST01", "FAKE-B");
    }

    @Test
    void badEntriesFailWithThePositionAndTheKindButNeverTheValues() {
        for (String spec : new String[] {"ok=TEST01;noequals", "=TEST01", "ok=", "ok=,,", "ok=test01", "ok=TOOLONG1",
                "ok=A;ok=B"}) {
            assertThatThrownBy(() -> SearchQuerySpec.parse(spec))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("번째 항목")
                    .hasMessageNotContaining("TEST01").hasMessageNotContaining("noequals")
                    .hasMessageNotContaining("TOOLONG1");
        }
        assertThatThrownBy(() -> SearchQuerySpec.parse("ok=TEST01; ;bad=abc")).hasMessageContaining("3번째");
    }

    @Test
    void aQueryOverTheTableLimitIsRejectedAndTheLimitItselfIsAllowed() {
        assertThat(SearchQuerySpec.parse("가".repeat(SearchQuerySpec.QUERY_MAX) + "=TEST01")).hasSize(1);
        assertThatThrownBy(() -> SearchQuerySpec.parse("가".repeat(SearchQuerySpec.QUERY_MAX + 1) + "=TEST01"))
                .hasMessageContaining("길이");
    }
}

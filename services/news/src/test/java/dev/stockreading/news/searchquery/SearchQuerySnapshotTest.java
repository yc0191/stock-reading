package dev.stockreading.news.searchquery;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.news.event.SearchQueriesPayload.Query;

/** 가짜 값(TEST 접두 종목코드, 가짜 검색어)만 쓴다. */
class SearchQuerySnapshotTest {

    @Test
    void theHashIsA64CharLowerCaseHexAndDoesNotDependOnTheReadOrder() {
        SearchQuerySnapshot first = new SearchQuerySnapshot(List.of(
                new Query("가짜 검색어", List.of("TEST01", "TEST02")), new Query("테스트 시황", List.of("*"))));
        SearchQuerySnapshot shuffled = new SearchQuerySnapshot(List.of(
                new Query("테스트 시황", List.of("*")), new Query("가짜 검색어", List.of("TEST02", "TEST01"))));

        assertThat(first.contentHash()).matches("[0-9a-f]{64}");
        assertThat(shuffled.contentHash()).isEqualTo(first.contentHash());
        assertThat(shuffled.queries()).containsExactlyElementsOf(first.queries());
    }

    @Test
    void anyChangeToTheQueriesOrTheirTagsChangesTheHash() {
        String base = new SearchQuerySnapshot(List.of(new Query("가짜 검색어", List.of("TEST01")))).contentHash();

        assertThat(new SearchQuerySnapshot(List.of(new Query("가짜 검색어 둘", List.of("TEST01")))).contentHash())
                .isNotEqualTo(base);
        assertThat(new SearchQuerySnapshot(List.of(new Query("가짜 검색어", List.of("TEST01", "TEST02")))).contentHash())
                .isNotEqualTo(base);
        assertThat(new SearchQuerySnapshot(List.of(new Query("가짜 검색어", List.of("*")))).contentHash())
                .isNotEqualTo(base);
        assertThat(new SearchQuerySnapshot(List.of()).contentHash()).isNotEqualTo(base);
    }

    @Test
    void aTagMovedToAnotherQueryChangesTheHashEvenWithTheSameQueriesAndTags() {
        // 검색어와 태그를 이어 붙이면 같아 보이는 두 목록도 구분되어야 한다.
        String one = new SearchQuerySnapshot(List.of(new Query("가", List.of("TEST01")),
                new Query("나", List.of("TEST02")))).contentHash();
        String other = new SearchQuerySnapshot(List.of(new Query("가", List.of("TEST01", "TEST02")),
                new Query("나", List.of("TEST02")))).contentHash();

        assertThat(one).isNotEqualTo(other);
    }
}

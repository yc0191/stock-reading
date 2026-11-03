package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NewsQueryTest {

    @Test
    void parsesQueriesWithSymbolsAndMarketWide() {
        List<NewsQuery> queries = NewsQuery.parse("테스트검색어=TEST01,TEST02; 테스트 시황 = * ");

        assertThat(queries).extracting(NewsQuery::query).containsExactly("테스트검색어", "테스트 시황");
        assertThat(queries.get(0).symbols()).containsExactly("TEST01", "TEST02");
        assertThat(queries.get(1).symbols()).containsExactly(NewsQuery.MARKET_WIDE);
    }

    @Test
    void skipsMalformedEntries() {
        assertThat(NewsQuery.parse("noequals;=TEST01;empty=;ok=A")).extracting(NewsQuery::query).containsExactly("ok");
    }

    @Test
    void blankSpecGivesNoQueries() {
        assertThat(NewsQuery.parse(null)).isEmpty();
        assertThat(NewsQuery.parse("  ")).isEmpty();
    }

    @Test
    void validatedParseAcceptsSixCharacterCodesAndMarketWide() {
        List<NewsQuery> queries = NewsQuery.parseValidated("테스트검색어=TEST01,A1B2C3;테스트시황=*");

        assertThat(queries).extracting(NewsQuery::query).containsExactly("테스트검색어", "테스트시황");
        assertThat(queries.get(0).symbols()).containsExactly("TEST01", "A1B2C3");
    }

    @Test
    void validatedParseOfBlankSpecIsEmptyNotAnError() {
        assertThat(NewsQuery.parseValidated(null)).isEmpty();
        assertThat(NewsQuery.parseValidated("")).isEmpty();
        assertThat(NewsQuery.parseValidated("  ")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"TEST0", "test01", "TEST012", "TEST-1", "TEST01,TEST0"})
    void validatedParseRejectsMalformedSymbolsWithoutEchoingTheSpec(String symbols) {
        String spec = "ok=TEST01;비공개검색어=" + symbols;

        assertThatThrownBy(() -> NewsQuery.parseValidated(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2번째")
                .hasMessageContaining("종목코드 형식 오류")
                .hasMessageNotContaining("비공개검색어")
                .hasMessageNotContaining(symbols);
    }

    @ParameterizedTest
    @ValueSource(strings = {"noequals", "=TEST01", "  =TEST01", "비공개검색어=", "비공개검색어= , ", "비공개검색어"})
    void validatedParseRejectsStructureErrorsWithoutEchoingTheSpec(String entry) {
        String spec = "ok=TEST01;" + entry;

        assertThatThrownBy(() -> NewsQuery.parseValidated(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2번째")
                .hasMessageContaining("구조 오류")
                .hasMessageNotContaining("비공개검색어")
                .hasMessageNotContaining("noequals");
    }

    @Test
    void validatedParseIgnoresBlankEntriesAndATrailingSemicolon() {
        List<NewsQuery> queries = NewsQuery.parseValidated("  ; ok=TEST01 ;; ok2=* ;  ;");

        assertThat(queries).extracting(NewsQuery::query).containsExactly("ok", "ok2");
    }

    @Test
    void validatedParseAcceptsASingleEntryWithATrailingSemicolon() {
        assertThat(NewsQuery.parseValidated("ok=TEST01;")).hasSize(1);
    }

    @Test
    void validatedParsePositionCountsBlankEntriesAndNamesTheKindOfError() {
        // 순번은 공백 항목도 센다: 1 정상, 2 공백, 3 정상, 4 구조 오류
        assertThatThrownBy(() -> NewsQuery.parseValidated("ok=TEST01; ;ok2=*;bad"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("4번째")
                .hasMessageContaining("구조 오류");
        // 1 정상, 2 구조 오류, 3 종목코드 오류: 첫 오류에서 멈춘다.
        assertThatThrownBy(() -> NewsQuery.parseValidated("ok=TEST01;noequals;bad=abc"))
                .hasMessageContaining("2번째")
                .hasMessageContaining("구조 오류");
        // 1 정상, 2 공백, 3 종목코드 오류
        assertThatThrownBy(() -> NewsQuery.parseValidated("ok=TEST01; ;bad=abc"))
                .hasMessageContaining("3번째")
                .hasMessageContaining("종목코드 형식 오류");
    }

    @Test
    void lenientParseStillSkipsMalformedEntriesInsteadOfFailing() {
        assertThat(NewsQuery.parse("noequals;=TEST01;empty=;ok=A;bad=abc"))
                .extracting(NewsQuery::query).containsExactly("ok", "bad");
    }

    @Test
    void lenientParseStillAcceptsAnySymbolText() {
        assertThat(NewsQuery.parse("ok=A").get(0).symbols()).containsExactly("A");
    }
}

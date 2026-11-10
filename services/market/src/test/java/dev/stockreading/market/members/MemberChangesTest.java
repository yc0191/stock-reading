package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class MemberChangesTest {

    private static final LocalDate SINCE = LocalDate.parse("2026-10-12");
    private static final LocalDate AS_OF = LocalDate.parse("2026-10-19");

    private static Member m(String symbol, int rank) {
        return new Member("US", symbol, null, "1.0", rank);
    }

    @Test
    void aStockThatLeftIsRemovedOnlyANewOneIsAddedOnlyAndARankChangeIsMovedOnly() {
        List<Member> before = List.of(m("AAA", 1), m("BBB", 2), m("CCC", 3));
        List<Member> after = List.of(m("BBB", 1), m("AAA", 2), m("DDD", 3));

        MemberChanges changes = MemberChanges.between(before, after, SINCE, AS_OF).orElseThrow();

        assertThat(changes.removed()).extracting(Member::symbol).containsExactly("CCC");
        assertThat(changes.removed().get(0).rank()).isEqualTo(3);
        assertThat(changes.added()).extracting(Member::symbol).containsExactly("DDD");
        assertThat(changes.moved()).extracting(MemberChanges.Moved::symbol, MemberChanges.Moved::rankBefore,
                MemberChanges.Moved::rankAfter).containsExactly(
                        org.assertj.core.groups.Tuple.tuple("BBB", 2, 1), org.assertj.core.groups.Tuple.tuple("AAA", 1, 2));
        assertThat(changes.since()).isEqualTo(SINCE);
        assertThat(changes.asOf()).isEqualTo(AS_OF);
    }

    @Test
    void aStockInTheSameRankIsNotListedAndWeightDifferencesAreIgnored() {
        List<Member> before = List.of(new Member("US", "AAA", null, "30.0", 1), m("BBB", 2));
        List<Member> after = List.of(new Member("US", "AAA", null, "29.0", 1), m("BBB", 2));

        assertThat(MemberChanges.between(before, after, SINCE, AS_OF)).isEmpty();
    }

    @Test
    void theSameSymbolInAnotherMarketIsADifferentStock() {
        List<Member> before = List.of(new Member("US", "AAA", null, null, 1));
        List<Member> after = List.of(new Member("KR", "AAA", null, null, 1));

        MemberChanges changes = MemberChanges.between(before, after, SINCE, AS_OF).orElseThrow();

        assertThat(changes.removed()).hasSize(1);
        assertThat(changes.added()).hasSize(1);
        assertThat(changes.moved()).isEmpty();
    }

    @Test
    void movedTakesTheNameFromTheNewCompositionAndFallsBackToTheOldOne() {
        List<Member> before = List.of(new Member("US", "AAA", "OLD NAME", null, 2), m("BBB", 1));
        List<Member> after = List.of(m("AAA", 1), m("BBB", 2));

        Optional<MemberChanges> changes = MemberChanges.between(before, after, SINCE, AS_OF);

        assertThat(changes.orElseThrow().moved().get(0).name()).isEqualTo("OLD NAME");
    }
}

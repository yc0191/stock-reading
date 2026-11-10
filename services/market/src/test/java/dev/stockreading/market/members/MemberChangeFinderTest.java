package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;

class MemberChangeFinderTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-16T17:00:00+09:00");

    private static MemberChangeFinder.Row row(String appliedAt, String first, String last, String... symbols) {
        java.util.ArrayList<Member> members = new java.util.ArrayList<>();
        for (String symbol : symbols) {
            members.add(new Member("US", symbol, null, null, members.size() + 1));
        }
        return new MemberChangeFinder.Row(OffsetDateTime.parse(appliedAt), LocalDate.parse(first),
                LocalDate.parse(last), members);
    }

    @Test
    void aChangeAppliedInsideTheRangeIsReportedAgainstTheCompositionBeforeIt() {
        List<MemberChangeFinder.Row> rows = List.of(
                row("2026-10-19T08:00:00+09:00", "2026-10-19", "2026-10-19", "AAA", "CCC"),
                row("2026-10-05T08:00:00+09:00", "2026-10-05", "2026-10-12", "AAA", "BBB"));

        MemberChanges changes = MemberChangeFinder.find(rows, SINCE).orElseThrow();

        assertThat(changes.since()).isEqualTo(LocalDate.parse("2026-10-12"));
        assertThat(changes.asOf()).isEqualTo(LocalDate.parse("2026-10-19"));
        assertThat(changes.removed()).extracting(Member::symbol).containsExactly("BBB");
        assertThat(changes.added()).extracting(Member::symbol).containsExactly("CCC");
    }

    @Test
    void aChangeAppliedBeforeTheRangeIsNotReported() {
        List<MemberChangeFinder.Row> rows = List.of(
                row("2026-10-10T08:00:00+09:00", "2026-10-10", "2026-10-15", "AAA", "CCC"),
                row("2026-10-05T08:00:00+09:00", "2026-10-05", "2026-10-09", "AAA", "BBB"));

        assertThat(MemberChangeFinder.find(rows, SINCE)).isEmpty();
    }

    @Test
    void theFirstCompositionEverReceivedIsNotAChange() {
        List<MemberChangeFinder.Row> rows = List.of(
                row("2026-10-19T08:00:00+09:00", "2026-10-19", "2026-10-19", "AAA", "BBB"));

        assertThat(MemberChangeFinder.find(rows, SINCE)).isEmpty();
        assertThat(MemberChangeFinder.find(List.of(), SINCE)).isEmpty();
    }

    @Test
    void severalChangesInTheRangeAreComparedFromBeforeTheFirstToTheLast() {
        List<MemberChangeFinder.Row> rows = List.of(
                row("2026-10-19T08:00:00+09:00", "2026-10-19", "2026-10-19", "AAA", "DDD"),
                row("2026-10-17T08:00:00+09:00", "2026-10-17", "2026-10-18", "AAA", "CCC"),
                row("2026-10-05T08:00:00+09:00", "2026-10-05", "2026-10-16", "AAA", "BBB"));

        MemberChanges changes = MemberChangeFinder.find(rows, SINCE).orElseThrow();

        assertThat(changes.since()).isEqualTo(LocalDate.parse("2026-10-16"));
        assertThat(changes.removed()).extracting(Member::symbol).containsExactly("BBB");
        assertThat(changes.added()).extracting(Member::symbol).containsExactly("DDD");
    }

    @Test
    void aCompositionThatChangedAndCameBackIsNotReported() {
        List<MemberChangeFinder.Row> rows = List.of(
                row("2026-10-19T08:00:00+09:00", "2026-10-19", "2026-10-19", "AAA", "BBB"),
                row("2026-10-17T08:00:00+09:00", "2026-10-17", "2026-10-18", "AAA", "CCC"),
                row("2026-10-05T08:00:00+09:00", "2026-10-05", "2026-10-16", "AAA", "BBB"));

        assertThat(MemberChangeFinder.find(rows, SINCE)).isEmpty();
    }

    @Test
    void whenTheWholeHistoryIsInsideTheRangeTheOldestRowIsTheBaseline() {
        List<MemberChangeFinder.Row> rows = List.of(
                row("2026-10-19T08:00:00+09:00", "2026-10-19", "2026-10-19", "AAA", "CCC"),
                row("2026-10-17T08:00:00+09:00", "2026-10-17", "2026-10-18", "AAA", "BBB"));

        MemberChanges changes = MemberChangeFinder.find(rows, SINCE).orElseThrow();

        assertThat(changes.since()).isEqualTo(LocalDate.parse("2026-10-18"));
        assertThat(changes.added()).extracting(Member::symbol).containsExactly("CCC");
    }
}

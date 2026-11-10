package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MemberFetchServiceTest {

    // 2026-11-09는 월요일이다.
    private static final LocalDate MONDAY = LocalDate.parse("2026-11-09");
    private static final LocalDate TUESDAY = LocalDate.parse("2026-11-10");

    private MemberSourcePort source;
    private MemberRepository repository;

    @BeforeEach
    void setUp() {
        source = mock(MemberSourcePort.class);
        repository = mock(MemberRepository.class);
        when(repository.inRebalanceMonth(any(), any())).thenReturn(false);
        when(repository.latest(anyLong())).thenReturn(Optional.empty());
    }

    private MemberFetchService service(String template, String fundIds) {
        return new MemberFetchService(new MemberSourceProperties(template, fundIds, null, 10, Duration.ZERO, null,
                DayOfWeek.MONDAY), source, repository);
    }

    private static List<Member> members(String... symbols) {
        java.util.ArrayList<Member> list = new java.util.ArrayList<>();
        for (String symbol : symbols) {
            list.add(new Member("US", symbol, null, "1.0", list.size() + 1));
        }
        return list;
    }

    private void tracked(MemberRepository.Target... targets) {
        when(repository.targets()).thenReturn(List.of(targets));
    }

    @Test
    void firstReceivedCompositionIsRecordedOnTheFetchDay() {
        tracked(new MemberRepository.Target(1, "TEST01", "TEST INDEX"));
        when(source.fetchTop(eq("F1"), eq(LocalDate.parse("2026-11-06")), any())).thenReturn(members("AAA", "BBB"));

        service("http://x/{fundId}", "TEST01:F1").run(MONDAY);

        verify(repository).record(1, members("AAA", "BBB"), MONDAY);
    }

    @Test
    void aSameCompositionOnlyMovesTheLastSeenDayEvenWhenWeightsDiffer() {
        tracked(new MemberRepository.Target(1, "TEST01", null));
        when(repository.latest(1)).thenReturn(Optional.of(new MemberRepository.Latest(7,
                List.of(new Member("US", "AAA", null, "9.0", 1), new Member("US", "BBB", null, "8.0", 2)),
                LocalDate.parse("2026-11-02"))));
        when(source.fetchTop(eq("F1"), any(), any())).thenReturn(members("AAA", "BBB"));

        service("http://x/{fundId}", "TEST01:F1").run(MONDAY);

        verify(repository).touch(7, 1, members("AAA", "BBB"), MONDAY);
        verify(repository, never()).record(anyLong(), any(), any());
    }

    @Test
    void aRankChangeAloneIsAChangeAndGetsANewHistoryRow() {
        tracked(new MemberRepository.Target(1, "TEST01", null));
        when(repository.latest(1)).thenReturn(Optional.of(new MemberRepository.Latest(7, members("AAA", "BBB"),
                LocalDate.parse("2026-11-02"))));
        when(source.fetchTop(eq("F1"), any(), any())).thenReturn(members("BBB", "AAA"));

        service("http://x/{fundId}", "TEST01:F1").run(MONDAY);

        verify(repository).record(1, members("BBB", "AAA"), MONDAY);
        verify(repository, never()).touch(anyLong(), anyLong(), any(), any());
    }

    @Test
    void aFailedFetchKeepsThePreviousCompositionAndDoesNotStopTheOtherEtfs() {
        tracked(new MemberRepository.Target(1, "TEST01", null), new MemberRepository.Target(2, "TEST02", null));
        when(source.fetchTop(eq("F1"), any(), any())).thenThrow(new MemberSourcePort.MemberSourceException("unexpected shape"));
        when(source.fetchTop(eq("F2"), any(), any())).thenReturn(members("CCC"));

        service("http://x/{fundId}", "TEST01:F1,TEST02:F2").run(MONDAY);

        verify(repository, never()).record(eq(1L), any(), any());
        verify(repository, never()).touch(anyLong(), anyLong(), any(), any());
        verify(repository).record(2, members("CCC"), MONDAY);
    }

    @Test
    void aDatabaseFailureOnOneEtfAlsoLeavesTheOthersRunning() {
        tracked(new MemberRepository.Target(1, "TEST01", null), new MemberRepository.Target(2, "TEST02", null));
        when(source.fetchTop(any(), any(), any())).thenReturn(members("AAA"));
        org.mockito.Mockito.doThrow(new IllegalStateException("db down")).when(repository)
                .record(eq(1L), any(), any());

        service("http://x/{fundId}", "TEST01:F1,TEST02:F2").run(MONDAY);

        verify(repository).record(eq(2L), any(), any());
    }

    @Test
    void onANonFetchDayNothingIsFetchedUnlessTheIndexRebalancesThisMonth() {
        tracked(new MemberRepository.Target(1, "TEST01", "TEST INDEX"), new MemberRepository.Target(2, "TEST02", "OTHER"));
        when(repository.latest(anyLong())).thenReturn(Optional.of(new MemberRepository.Latest(7,
                List.of(new Member("US", "AAA", null, "9.0", 1)), MONDAY)));
        when(repository.inRebalanceMonth("TEST INDEX", TUESDAY)).thenReturn(true);
        when(source.fetchTop(eq("F1"), any(), any())).thenReturn(members("AAA"));

        service("http://x/{fundId}", "TEST01:F1,TEST02:F2").run(TUESDAY);

        verify(source).fetchTop(eq("F1"), eq(MONDAY), any());
        verify(source, never()).fetchTop(eq("F2"), any(), any());
    }

    @Test
    void anEtfWithNoCompositionYetIsFetchedWithoutWaitingForTheFetchDay() {
        tracked(new MemberRepository.Target(1, "TEST01", null));
        when(source.fetchTop(eq("F1"), any(), any())).thenReturn(members("AAA"));

        service("http://x/{fundId}", "TEST01:F1").run(TUESDAY);

        verify(repository).record(1, members("AAA"), TUESDAY);
    }

    @Test
    void etfsWithoutAFundIdAreLeftToManualEditing() {
        tracked(new MemberRepository.Target(1, "TEST01", null), new MemberRepository.Target(3, "TEST03", null));
        when(source.fetchTop(eq("F1"), any(), any())).thenReturn(members("AAA"));

        service("http://x/{fundId}", "TEST01:F1").run(MONDAY);

        verify(source).fetchTop(eq("F1"), any(), any());
        verify(repository, never()).record(eq(3L), any(), any());
    }

    @Test
    void doesNothingWithoutASourceTemplate() {
        service("", "TEST01:F1").run(MONDAY);

        verifyNoInteractions(source, repository);
    }

    @Test
    void theBaseDateIsTheWeekdayBeforeToday() {
        assertThat(MemberFetchService.baseDate(LocalDate.parse("2026-11-09"))).isEqualTo(LocalDate.parse("2026-11-06"));
        assertThat(MemberFetchService.baseDate(LocalDate.parse("2026-11-10"))).isEqualTo(LocalDate.parse("2026-11-09"));
        assertThat(MemberFetchService.baseDate(LocalDate.parse("2026-11-14"))).isEqualTo(LocalDate.parse("2026-11-13"));
        assertThat(MemberFetchService.baseDate(LocalDate.parse("2026-11-15"))).isEqualTo(LocalDate.parse("2026-11-13"));
    }

    @Test
    void anEtfNotSeenForMoreThanSevenDaysIsFetchedOnAnyWeekday() {
        tracked(new MemberRepository.Target(1, "TEST01", null), new MemberRepository.Target(2, "TEST02", null));
        // 8일 전이면 받고, 7일 전이면 아직 받지 않는다.
        when(repository.latest(1)).thenReturn(Optional.of(new MemberRepository.Latest(7, members("AAA"),
                TUESDAY.minusDays(8))));
        when(repository.latest(2)).thenReturn(Optional.of(new MemberRepository.Latest(8, members("BBB"),
                TUESDAY.minusDays(7))));
        when(source.fetchTop(any(), any(), any())).thenReturn(members("AAA"));

        service("http://x/{fundId}", "TEST01:F1,TEST02:F2").run(TUESDAY);

        verify(source).fetchTop(eq("F1"), any(), any());
        verify(source, never()).fetchTop(eq("F2"), any(), any());
    }

    @Test
    void theResponseRawIsSavedThroughTheRecorderTheSourceCalls() {
        tracked(new MemberRepository.Target(1, "TEST01", null));
        when(source.fetchTop(eq("F1"), any(), any())).thenAnswer(invocation -> {
            invocation.<MemberSourcePort.RawRecorder>getArgument(2).record(503, "down");
            throw new MemberSourcePort.MemberSourceException("unexpected status 503");
        });

        service("http://x/{fundId}", "TEST01:F1").run(MONDAY);

        verify(repository).saveRaw(1, LocalDate.parse("2026-11-06"), 503, "down");
        verify(repository, never()).record(anyLong(), any(), any());
    }
}

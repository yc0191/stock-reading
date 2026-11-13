package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** 받은 결과의 원문 저장·비교·반영 규칙을 가짜 저장소로 본다. 비교 규칙은 시세처리기가 직접 받던 때와 같다. */
class MemberApplyServiceTest {

    private static final UUID EVENT = UUID.fromString("7a2b3c4d-5e6f-4a1b-9c2d-3e4f5a6b7c80");
    private static final LocalDate MONDAY = LocalDate.parse("2026-11-09");

    private MemberRepository repository;
    private MemberApplyService service;

    @BeforeEach
    void setUp() {
        repository = mock(MemberRepository.class);
        service = new MemberApplyService(repository, new TransactionTemplate(mock(PlatformTransactionManager.class)));
        when(repository.etfId("TEST01")).thenReturn(Optional.of(1L));
        when(repository.saveRaw(any(), anyLong(), any(), org.mockito.ArgumentMatchers.anyInt(), anyString(), any()))
                .thenReturn(true);
        when(repository.latest(1)).thenReturn(Optional.empty());
    }

    private static List<Member> members(String... symbols) {
        List<Member> list = new ArrayList<>();
        for (String symbol : symbols) {
            list.add(new Member("US", symbol, null, "1.0", list.size() + 1));
        }
        return list;
    }

    private static MembersFetchedEnvelope.Payload ok(List<Member> members) {
        return new MembersFetchedEnvelope.Payload("TEST01", "2026-11-06", "2026-11-09", "2026-11-09T08:00:01+09:00",
                200, " {\"pdf\": 1}\n", members, null);
    }

    @Test
    void theFirstCompositionIsRecordedOnTheFetchDayAndItsRawIsSavedVerbatim() {
        assertThat(service.apply(EVENT, ok(members("AAA", "BBB")))).isEqualTo(MemberApplyService.Result.FIRST);

        verify(repository).saveRaw(EVENT, 1, LocalDate.parse("2026-11-06"), 200, " {\"pdf\": 1}\n",
                OffsetDateTime.parse("2026-11-09T08:00:01+09:00"));
        verify(repository).record(1, members("AAA", "BBB"), MONDAY);
    }

    @Test
    void aSameCompositionOnlyMovesTheLastSeenDayEvenWhenWeightsDiffer() {
        when(repository.latest(1)).thenReturn(Optional.of(new MemberRepository.Latest(7,
                List.of(new Member("US", "AAA", null, "9.0", 1), new Member("US", "BBB", null, "8.0", 2)),
                LocalDate.parse("2026-11-02"))));

        assertThat(service.apply(EVENT, ok(members("AAA", "BBB")))).isEqualTo(MemberApplyService.Result.SAME);

        verify(repository).touch(7, 1, members("AAA", "BBB"), MONDAY);
        verify(repository, never()).record(anyLong(), any(), any());
    }

    @Test
    void aRankChangeAloneIsAChangeAndGetsANewHistoryRow() {
        when(repository.latest(1)).thenReturn(Optional.of(new MemberRepository.Latest(7, members("AAA", "BBB"),
                LocalDate.parse("2026-11-02"))));

        assertThat(service.apply(EVENT, ok(members("BBB", "AAA")))).isEqualTo(MemberApplyService.Result.CHANGED);

        verify(repository).record(1, members("BBB", "AAA"), MONDAY);
        verify(repository, never()).touch(anyLong(), anyLong(), any(), any());
    }

    @Test
    void aFailedFetchSavesOnlyTheRawAndKeepsThePreviousComposition() {
        MembersFetchedEnvelope.Payload failed = new MembersFetchedEnvelope.Payload("TEST01", "2026-11-06",
                "2026-11-09", "2026-11-09T08:00:01+09:00", 503, "down", null, "unexpected status 503");

        assertThat(service.apply(EVENT, failed)).isEqualTo(MemberApplyService.Result.FAILED_KEPT);

        verify(repository).saveRaw(EVENT, 1, LocalDate.parse("2026-11-06"), 503, "down",
                OffsetDateTime.parse("2026-11-09T08:00:01+09:00"));
        verify(repository, never()).record(anyLong(), any(), any());
        verify(repository, never()).touch(anyLong(), anyLong(), any(), any());
    }

    @Test
    void aRedeliveredEventIsNotAppliedTwice() {
        when(repository.saveRaw(any(), anyLong(), any(), org.mockito.ArgumentMatchers.anyInt(), anyString(), any()))
                .thenReturn(false);

        assertThat(service.apply(EVENT, ok(members("AAA")))).isEqualTo(MemberApplyService.Result.DUPLICATE);

        verify(repository, never()).record(anyLong(), any(), any());
        verify(repository, never()).latest(anyLong());
    }

    @Test
    void anEtfThisServiceDoesNotKnowIsIgnored() {
        when(repository.etfId("TEST01")).thenReturn(Optional.empty());

        assertThat(service.apply(EVENT, ok(members("AAA")))).isEqualTo(MemberApplyService.Result.UNKNOWN_ETF);

        verify(repository, never()).saveRaw(any(), anyLong(), any(), org.mockito.ArgumentMatchers.anyInt(), anyString(), any());
    }
}

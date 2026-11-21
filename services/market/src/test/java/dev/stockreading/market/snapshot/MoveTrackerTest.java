package dev.stockreading.market.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.market.MarketProperties;

class MoveTrackerTest {

    private static final OffsetDateTime T0 = OffsetDateTime.parse("2026-10-20T16:00:00+09:00");

    private MoveDetector detector;
    private MoveStateRepository states;
    private MovePublisher publisher;
    private MoveTracker tracker;
    // 저장소 대역이 기억하는 상태. 재시작은 새 MoveTracker를 같은 저장소에 붙여서 흉내 낸다.
    private final Map<DetectionType, MoveState> stored = new HashMap<>();
    private LastPublish lastPublish;

    @BeforeEach
    void setUp() {
        detector = mock(MoveDetector.class);
        states = mock(MoveStateRepository.class);
        publisher = mock(MovePublisher.class);
        when(states.find(anyString(), any(DetectionType.class)))
                .thenAnswer(inv -> Optional.ofNullable(stored.get(inv.<DetectionType>getArgument(1))));
        doAnswer(inv -> {
            stored.put(inv.getArgument(1), new MoveState(inv.getArgument(2), inv.getArgument(3), inv.getArgument(4)));
            return null;
        }).when(states).save(anyString(), any(DetectionType.class), anyLong(), any(OffsetDateTime.class), any(OffsetDateTime.class));
        doAnswer(inv -> {
            stored.remove(inv.<DetectionType>getArgument(1));
            return null;
        }).when(states).delete(anyString(), any(DetectionType.class));
        doAnswer(inv -> {
            stored.computeIfPresent(inv.<DetectionType>getArgument(1), (type, state) -> endedAt(state, inv.getArgument(2)));
            return null;
        }).when(states).markEnded(anyString(), any(DetectionType.class), any(OffsetDateTime.class));
        when(states.findLastPublish(anyString())).thenAnswer(inv -> Optional.ofNullable(lastPublish));
        doAnswer(inv -> {
            lastPublish = new LastPublish(inv.getArgument(1), inv.getArgument(2));
            return null;
        }).when(states).saveLastPublish(anyString(), anyLong(), any(OffsetDateTime.class));
        // 아래 기존 시험은 상태 규칙만 보도록 연속 확인(1건)과 재발행 간격(끔)을 꺼 둔다.
        tracker = new MoveTracker(detector, states, publisher, properties(1, 0));
    }

    // 저장소의 markEnded와 같은 조건: 진행 중이고 마지막 발행보다 늦게 풀렸을 때만 풀린 시각을 넣는다.
    static MoveState endedAt(MoveState state, OffsetDateTime endedAt) {
        if (state.ended() || !endedAt.isAfter(state.lastPublishedAt())) {
            return state;
        }
        return new MoveState(state.lastPublishedPrice(), endedAt, state.lastPublishedAt());
    }

    private static MarketProperties properties(int confirmCount, int intervalMinutes) {
        return new MarketProperties(5, new BigDecimal("1.00"), List.of("fake.a"), new BigDecimal("3.00"), List.of(),
                confirmCount, new BigDecimal("1.00"), List.of("fake.volume", "fake.open"), intervalMinutes, new BigDecimal("2.00"), new BigDecimal("7.00"));
    }

    private static Detection detection(DetectionType type, long price, OffsetDateTime startedAt) {
        return new Detection(type, 10_000, price, BigDecimal.ZERO, new BigDecimal("1.00"), startedAt);
    }

    private void snapshot(OffsetDateTime asOf, long price, Evaluation... evaluations) {
        when(detector.evaluate("TEST01", asOf, price, null)).thenReturn(List.of(evaluations));
        tracker.onSnapshot("TEST01", asOf, price, null, "corr-1");
    }

    private static Evaluation window(long price, OffsetDateTime startedAt) {
        return Evaluation.detected(detection(DetectionType.WINDOW, price, startedAt));
    }

    @Test
    void firstDetectionIsPublishedAndRemembered() {
        OffsetDateTime start = T0.minusMinutes(2);

        snapshot(T0, 10_200, window(10_200, start));

        verify(publisher).publish(eq("TEST01"), any(Detection.class), eq(start), eq(T0), eq("corr-1"));
        assertThat(stored.get(DetectionType.WINDOW)).isEqualTo(new MoveState(10_200, start, T0));
    }

    @Test
    void smallerFurtherMoveIsNotPublishedAgain() {
        snapshot(T0, 10_200, window(10_200, T0));

        // 10200 -> 10301은 +0.99%, 임계값 1.00% 미만
        snapshot(T0.plusSeconds(1), 10_301, window(10_301, T0));

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
        assertThat(stored.get(DetectionType.WINDOW).lastPublishedPrice()).isEqualTo(10_200L);
    }

    @Test
    void moveOfExactlyTheThresholdFromTheLastPublishedPriceIsPublishedAgainKeepingTheStartTime() {
        OffsetDateTime start = T0.minusMinutes(1);
        snapshot(T0, 10_000, window(10_000, start));

        // 10000 -> 10100은 정확히 +1.00%
        snapshot(T0.plusSeconds(1), 10_100, window(10_100, T0.plusSeconds(1)));

        ArgumentCaptor<OffsetDateTime> started = ArgumentCaptor.forClass(OffsetDateTime.class);
        verify(publisher, times(2)).publish(anyString(), any(), started.capture(), any(), anyString());
        assertThat(started.getAllValues()).containsExactly(start, start);
        assertThat(stored.get(DetectionType.WINDOW).lastPublishedPrice()).isEqualTo(10_100L);
    }

    @Test
    void movingBackByTheThresholdWhileStillDetectedIsPublishedAgain() {
        snapshot(T0, 10_300, window(10_300, T0));

        // 10300 -> 10197은 -1.00%지만 시작 가격 대비로는 아직 감지 상태
        snapshot(T0.plusSeconds(1), 10_197, window(10_197, T0));

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void aCrossingAfterTheConditionEndsIsPublishedOnlyWhenItMovedTheThresholdFromTheLastPublishedPrice() {
        snapshot(T0, 10_200, window(10_200, T0));
        snapshot(T0.plusSeconds(1), 10_020, Evaluation.clear(DetectionType.WINDOW));
        assertThat(stored.get(DetectionType.WINDOW)).isEqualTo(new MoveState(10_200, T0.plusSeconds(1), T0));

        // 이전 발행 가격(10200)과 거의 같으면 다시 넘어도 발행하지 않는다.
        snapshot(T0.plusSeconds(2), 10_210, window(10_210, T0.plusSeconds(2)));
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());

        // 10200 -> 10302는 +1.00%. 풀린 뒤의 변동이므로 시작 시각은 다시 넘은 첫 스냅샷으로 새로 잡는다.
        OffsetDateTime crossedAgain = T0.plusSeconds(2);
        OffsetDateTime published = T0.plusSeconds(3);
        snapshot(published, 10_302, window(10_302, published));

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
        verify(publisher).publish(eq("TEST01"), any(), eq(crossedAgain), eq(published), anyString());
        assertThat(stored.get(DetectionType.WINDOW)).isEqualTo(new MoveState(10_302, crossedAgain, published));
    }

    @Test
    void unknownEvaluationLeavesTheStateAlone() {
        snapshot(T0, 10_200, Evaluation.detected(detection(DetectionType.PREV_CLOSE, 10_200, T0)));

        snapshot(T0.plusSeconds(1), 10_210, Evaluation.unknown(DetectionType.PREV_CLOSE));

        assertThat(stored).containsKey(DetectionType.PREV_CLOSE);
        verify(states, never()).delete(anyString(), any(DetectionType.class));
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void stateSurvivesARestartBecauseItLivesInTheRepository() {
        snapshot(T0, 10_200, window(10_200, T0));

        MoveTracker restarted = new MoveTracker(detector, states, publisher, properties(1, 0));
        OffsetDateTime next = T0.plusSeconds(1);
        when(detector.evaluate("TEST01", next, 10_250, null)).thenReturn(List.of(window(10_250, next)));
        restarted.onSnapshot("TEST01", next, 10_250, null, "corr-2");

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void typesKeepSeparateStates() {
        OffsetDateTime start = T0.minusMinutes(1);

        snapshot(T0, 10_400, window(10_400, start), Evaluation.detected(detection(DetectionType.PREV_CLOSE, 10_400, T0)));

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
        assertThat(stored.get(DetectionType.WINDOW).moveStartedAt()).isEqualTo(start);
        assertThat(stored.get(DetectionType.PREV_CLOSE).moveStartedAt()).isEqualTo(T0);
    }

    @Test
    void prevCloseKeepsTheFirstCrossingTimeOnRepublish() {
        snapshot(T0, 10_300, Evaluation.detected(detection(DetectionType.PREV_CLOSE, 10_300, T0)));

        OffsetDateTime later = T0.plusMinutes(10);
        snapshot(later, 10_700, Evaluation.detected(detection(DetectionType.PREV_CLOSE, 10_700, later)));

        verify(publisher).publish(eq("TEST01"), any(), eq(T0), eq(later), anyString());
    }

    // ---- 연속 확인

    private MoveTracker confirming() {
        return new MoveTracker(detector, states, publisher, properties(3, 30));
    }

    private void snapshot(MoveTracker target, OffsetDateTime asOf, long price, Evaluation... evaluations) {
        when(detector.evaluate("TEST01", asOf, price, null)).thenReturn(List.of(evaluations));
        target.onSnapshot("TEST01", asOf, price, null, "corr-1");
    }

    private static Evaluation up(DetectionType type, long price, OffsetDateTime startedAt) {
        return Evaluation.detected(new Detection(type, 10_000, price, new BigDecimal("2.00"), new BigDecimal("1.00"), startedAt));
    }

    private static Evaluation down(DetectionType type, long price, OffsetDateTime startedAt) {
        return Evaluation.detected(new Detection(type, 10_000, price, new BigDecimal("-2.00"), new BigDecimal("1.00"), startedAt));
    }

    @Test
    void aMoveIsPublishedOnlyAtTheThirdConsecutiveDetectionAndStartsAtTheEarlierTime() {
        MoveTracker target = confirming();

        snapshot(target, T0, 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0.plusSeconds(1)));
        verify(publisher, never()).publish(anyString(), any(), any(), any(), anyString());

        snapshot(target, T0.plusSeconds(2), 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0.plusSeconds(2)));

        // 유형 2의 변동 시각은 감지한 스냅샷이라 세 번째 시각이지만, 연속 확인이 시작된 첫 스냅샷으로 앞당긴다.
        verify(publisher, times(1)).publish(eq("TEST01"), any(Detection.class), eq(T0), eq(T0.plusSeconds(2)), eq("corr-1"));
    }

    @Test
    void aClearedSnapshotInTheMiddleStartsTheCountOver() {
        MoveTracker target = confirming();

        snapshot(target, T0, 10_200, up(DetectionType.WINDOW, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_200, up(DetectionType.WINDOW, 10_200, T0));
        snapshot(target, T0.plusSeconds(2), 10_000, Evaluation.clear(DetectionType.WINDOW));
        snapshot(target, T0.plusSeconds(3), 10_200, up(DetectionType.WINDOW, 10_200, T0));
        snapshot(target, T0.plusSeconds(4), 10_200, up(DetectionType.WINDOW, 10_200, T0));

        verify(publisher, never()).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void anUnknownSnapshotInTheMiddleStartsTheCountOver() {
        MoveTracker target = confirming();

        // 감지, 판단 불가, 감지, 판단 불가, 감지는 연속 세 건이 아니다.
        snapshot(target, T0, 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_200, Evaluation.unknown(DetectionType.PREV_CLOSE));
        snapshot(target, T0.plusSeconds(2), 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));
        snapshot(target, T0.plusSeconds(3), 10_200, Evaluation.unknown(DetectionType.PREV_CLOSE));
        snapshot(target, T0.plusSeconds(4), 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));
        verify(publisher, never()).publish(anyString(), any(), any(), any(), anyString());

        // 판단 불가 뒤 새로 세 건이 이어지면 발행한다.
        snapshot(target, T0.plusSeconds(5), 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));
        snapshot(target, T0.plusSeconds(6), 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void aChangeOfDirectionStartsTheCountOver() {
        MoveTracker target = confirming();

        snapshot(target, T0, 10_200, up(DetectionType.WINDOW, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_200, up(DetectionType.WINDOW, 10_200, T0));
        snapshot(target, T0.plusSeconds(2), 9_800, down(DetectionType.WINDOW, 9_800, T0));

        verify(publisher, never()).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void eachTypeCountsItsOwnStreak() {
        MoveTracker target = confirming();

        snapshot(target, T0, 10_200, up(DetectionType.WINDOW, 10_200, T0), up(DetectionType.PREV_CLOSE, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_200, up(DetectionType.WINDOW, 10_200, T0), Evaluation.clear(DetectionType.PREV_CLOSE));
        snapshot(target, T0.plusSeconds(2), 10_200, up(DetectionType.WINDOW, 10_200, T0), up(DetectionType.PREV_CLOSE, 10_200, T0));

        ArgumentCaptor<Detection> published = ArgumentCaptor.forClass(Detection.class);
        verify(publisher, times(1)).publish(anyString(), published.capture(), any(), any(), anyString());
        assertThat(published.getValue().type()).isEqualTo(DetectionType.WINDOW);
    }

    @Test
    void aSingleSpikeInsideAnOngoingMoveIsNotPublishedNorIsTheReturnFromIt() {
        // 운영에서 본 모양: 기준을 넘은 상태가 이어지는 중에 현재가 한 건만 튀었다가 다음 스냅샷에 제자리로 돌아온다.
        // 상태만 세면 튄 값이 연속 확인을 이어받아 발행되고, 돌아온 값도 튄 값 대비 다시 크게 움직여 한 번 더 발행된다.
        MoveTracker target = confirming();
        for (int i = 0; i < 3; i++) {
            snapshot(target, T0.plusSeconds(i), 10_400, up(DetectionType.PREV_CLOSE, 10_400, T0));
        }
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());

        OffsetDateTime later = T0.plusMinutes(150);
        snapshot(target, later, 10_400, up(DetectionType.PREV_CLOSE, 10_400, T0));
        snapshot(target, later.plusSeconds(2), 10_850, up(DetectionType.PREV_CLOSE, 10_850, T0));
        snapshot(target, later.plusSeconds(4), 10_400, up(DetectionType.PREV_CLOSE, 10_400, T0));
        snapshot(target, later.plusSeconds(6), 10_400, up(DetectionType.PREV_CLOSE, 10_400, T0));
        snapshot(target, later.plusSeconds(8), 10_400, up(DetectionType.PREV_CLOSE, 10_400, T0));

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void aJumpThatHoldsIsPublishedOnceThreeSimilarPricesFollow() {
        MoveTracker target = confirming();
        for (int i = 0; i < 3; i++) {
            snapshot(target, T0.plusSeconds(i), 10_400, up(DetectionType.PREV_CLOSE, 10_400, T0));
        }

        OffsetDateTime later = T0.plusMinutes(150);
        snapshot(target, later, 10_850, up(DetectionType.PREV_CLOSE, 10_850, T0));
        snapshot(target, later.plusSeconds(2), 10_860, up(DetectionType.PREV_CLOSE, 10_860, T0));
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());

        snapshot(target, later.plusSeconds(4), 10_850, up(DetectionType.PREV_CLOSE, 10_850, T0));

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), eq("corr-1"));
        assertThat(lastPublish.price()).isEqualTo(10_850L);
    }

    @Test
    void confirmingPricesExactlyTheSpreadPercentApartArePublished() {
        MoveTracker target = confirming();

        // 가장 낮은 10200 대비 10302는 정확히 +1.00%(허용 폭)
        snapshot(target, T0, 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_302, up(DetectionType.PREV_CLOSE, 10_302, T0));
        snapshot(target, T0.plusSeconds(2), 10_250, up(DetectionType.PREV_CLOSE, 10_250, T0));

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void confirmingPricesJustOverTheSpreadPercentApartAreNotPublished() {
        MoveTracker target = confirming();

        // 가장 낮은 10200 대비 10303은 +1.01%로 허용 폭을 넘는다.
        snapshot(target, T0, 10_200, up(DetectionType.PREV_CLOSE, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_303, up(DetectionType.PREV_CLOSE, 10_303, T0));
        snapshot(target, T0.plusSeconds(2), 10_250, up(DetectionType.PREV_CLOSE, 10_250, T0));

        verify(publisher, never()).publish(anyString(), any(), any(), any(), anyString());
    }

    // ---- 종목 단위 재발행 간격

    private MoveTracker publishedOnceAt(OffsetDateTime at, long price) {
        MoveTracker target = confirming();
        for (int i = 0; i < 3; i++) {
            snapshot(target, at.plusSeconds(i), price, up(DetectionType.WINDOW, price, at));
        }
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
        return target;
    }

    private void threeDetections(MoveTracker target, DetectionType type, OffsetDateTime at, long price) {
        for (int i = 0; i < 3; i++) {
            snapshot(target, at.plusSeconds(i), price, up(type, price, at));
        }
    }

    @Test
    void aMoveWithinTheIntervalIsHeldBackWhenItIsLessThanTheExceptionPercentFromTheLastPublishedPrice() {
        MoveTracker target = publishedOnceAt(T0, 10_200);
        snapshot(target, T0.plusMinutes(5), 10_200, Evaluation.clear(DetectionType.WINDOW));

        // 10200 -> 10395는 +1.91%
        threeDetections(target, DetectionType.WINDOW, T0.plusMinutes(10), 10_395);

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void aMoveWithinTheIntervalIsPublishedWhenItIsTheExceptionPercentFromTheLastPublishedPrice() {
        MoveTracker target = publishedOnceAt(T0, 10_200);
        snapshot(target, T0.plusMinutes(5), 10_200, Evaluation.clear(DetectionType.WINDOW));

        // 10200 -> 10404는 정확히 +2.00%
        threeDetections(target, DetectionType.WINDOW, T0.plusMinutes(10), 10_404);

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
        assertThat(lastPublish.price()).isEqualTo(10_404L);
    }

    @Test
    void theIntervalAppliesToTheOtherTypeToo() {
        MoveTracker target = publishedOnceAt(T0, 10_200);

        threeDetections(target, DetectionType.PREV_CLOSE, T0.plusMinutes(10), 10_320);

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    // 유형 1이 T0에 발행한 같은 순간부터 유형 2 조건이 이어진다. 유형 2는 간격 때문에 막히고 상태가 남는다.
    private MoveTracker windowPublishedWhilePrevCloseIsHeldBack() {
        MoveTracker target = confirming();
        for (int i = 0; i < 3; i++) {
            snapshot(target, T0.plusSeconds(i), 10_400,
                    up(DetectionType.WINDOW, 10_400, T0), up(DetectionType.PREV_CLOSE, 10_400, T0));
        }
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
        return target;
    }

    @Test
    void aMoveHeldBackByTheIntervalIsNotPublishedWhenTheIntervalEndsWithoutNewMovement() {
        MoveTracker target = windowPublishedWhilePrevCloseIsHeldBack();
        assertThat(stored).containsKey(DetectionType.PREV_CLOSE);

        // 간격(30분)이 끝난 뒤에도 가격이 막은 때와 같고 유형 2 조건이 계속 이어진다.
        OffsetDateTime after = T0.plusSeconds(2).plusMinutes(31);
        for (int i = 0; i < 5; i++) {
            snapshot(target, after.plusSeconds(i), 10_400, up(DetectionType.PREV_CLOSE, 10_400, T0));
        }

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void aMoveHeldBackByTheIntervalIsPublishedWhenItGoesTheExceptionPercentFurtherInsideTheInterval() {
        MoveTracker target = windowPublishedWhilePrevCloseIsHeldBack();

        // 마지막 발행 가격 10400 -> 10608은 정확히 +2.00%
        for (int i = 0; i < 3; i++) {
            snapshot(target, T0.plusMinutes(10).plusSeconds(i), 10_608, up(DetectionType.PREV_CLOSE, 10_608, T0));
        }

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void aHeldBackTypeIsPublishedAfterItsConditionClearsOnlyWhenItMovedTheThresholdFromTheHeldPrice() {
        MoveTracker target = windowPublishedWhilePrevCloseIsHeldBack();
        snapshot(target, T0.plusMinutes(5), 10_000, Evaluation.clear(DetectionType.PREV_CLOSE));
        assertThat(stored.get(DetectionType.PREV_CLOSE).ended()).isTrue();

        OffsetDateTime after = T0.plusSeconds(2).plusMinutes(31);
        threeDetections(target, DetectionType.PREV_CLOSE, after, 10_400);
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());

        // 막은 가격 10400 -> 10504는 +1.00%
        threeDetections(target, DetectionType.PREV_CLOSE, after.plusMinutes(1), 10_504);
        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void onlyOneTypeIsPublishedWhenBothAreConfirmedOnTheSameSnapshot() {
        MoveTracker target = confirming();

        for (int i = 0; i < 3; i++) {
            snapshot(target, T0.plusSeconds(i), 10_400,
                    up(DetectionType.WINDOW, 10_400, T0), up(DetectionType.PREV_CLOSE, 10_400, T0));
        }

        ArgumentCaptor<Detection> published = ArgumentCaptor.forClass(Detection.class);
        verify(publisher, times(1)).publish(anyString(), published.capture(), any(), any(), anyString());
        assertThat(published.getValue().type()).isEqualTo(DetectionType.WINDOW);
    }

    @Test
    void aMoveExactlyAtTheEndOfTheIntervalIsPublishedAndOneSecondBeforeIsNot() {
        MoveTracker target = publishedOnceAt(T0, 10_200);
        snapshot(target, T0.plusMinutes(5), 10_200, Evaluation.clear(DetectionType.WINDOW));
        // 마지막 발행은 세 번째 스냅샷(T0+2초)의 시각이다. 연속 확인이 끝나는 세 번째 스냅샷이 간격이 끝나기 1초 전에 온다.
        OffsetDateTime justBefore = T0.plusSeconds(2).plusMinutes(30).minusSeconds(3);

        snapshot(target, justBefore, 10_250, up(DetectionType.WINDOW, 10_250, justBefore));
        snapshot(target, justBefore.plusSeconds(1), 10_250, up(DetectionType.WINDOW, 10_250, justBefore));
        snapshot(target, justBefore.plusSeconds(2), 10_250, up(DetectionType.WINDOW, 10_250, justBefore));
        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());

        // 막힌 변동은 막은 가격(10250)을 기준으로 남으므로, 간격이 끝나는 정각에 임계값(1%)만큼 더 움직인 값(10353)이 와야 나간다.
        snapshot(target, justBefore.plusSeconds(3), 10_353, up(DetectionType.WINDOW, 10_353, justBefore));

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void theIntervalIsOffWhenItIsZeroMinutes() {
        MoveTracker target = new MoveTracker(detector, states, publisher, properties(1, 0));
        snapshot(target, T0, 10_200, up(DetectionType.WINDOW, 10_200, T0));
        snapshot(target, T0.plusSeconds(1), 10_200, Evaluation.clear(DetectionType.WINDOW));

        // 10200 -> 10302는 +1.00%로 유형 상태는 넘지만, 간격이 켜져 있었다면 예외 폭(2%) 미만이라 막혔을 값이다.
        snapshot(target, T0.plusSeconds(2), 10_302, up(DetectionType.WINDOW, 10_302, T0));

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
    }

    @Test
    void theLastPublishedPriceSurvivesAClearBecauseItIsKeptPerSymbol() {
        MoveTracker target = publishedOnceAt(T0, 10_200);
        snapshot(target, T0.plusMinutes(1), 10_000, Evaluation.clear(DetectionType.WINDOW));

        assertThat(stored.get(DetectionType.WINDOW).ended()).isTrue();
        assertThat(lastPublish).isEqualTo(new LastPublish(10_200, T0.plusSeconds(2)));
    }

    @Test
    void aHeldBackMoveIsLoggedOnceWhileItKeepsBeingDetected() {
        Logger logger = (Logger) LoggerFactory.getLogger(MoveTracker.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            MoveTracker target = publishedOnceAt(T0, 10_200);
            snapshot(target, T0.plusMinutes(5), 10_200, Evaluation.clear(DetectionType.WINDOW));
            // 10200 -> 10302는 +1.00%로 유형 상태는 넘지만 간격 예외 폭(2%)에는 못 미친다.
            for (int i = 0; i < 20; i++) {
                snapshot(target, T0.plusMinutes(10).plusSeconds(i), 10_302, up(DetectionType.WINDOW, 10_302, T0));
            }

            assertThat(appender.list).extracting(ILoggingEvent::getFormattedMessage)
                    .filteredOn(message -> message.contains("not published")).hasSize(1);
        } finally {
            logger.detachAppender(appender);
        }
    }

    // 11-17·11-18 운영: 유형 2 기준선 언저리를 오르내린 종목이 더 움직이지 않았는데도 간격이 지날 때마다 다시 발행됐다.
    @Test
    void aSymbolHoveringAroundTheThresholdIsNotPublishedAgainAfterTheIntervalWithoutFurtherMovement() {
        MoveTracker target = confirming();
        threeDetections(target, DetectionType.PREV_CLOSE, T0, 10_300);
        snapshot(target, T0.plusMinutes(5), 10_290, Evaluation.clear(DetectionType.PREV_CLOSE));

        // 간격(30분)이 지난 뒤 같은 기준선을 다시 넘었지만 10300 -> 10320은 +0.19%뿐이다.
        threeDetections(target, DetectionType.PREV_CLOSE, T0.plusMinutes(40), 10_320);

        verify(publisher, times(1)).publish(anyString(), any(), any(), any(), anyString());
    }

    // 11-16 운영: 금요일 15:05의 상태가 주말을 넘겨 남아, 월요일 첫 감지 메일의 변동 시각이 금요일로 나갔다.
    private static final OffsetDateTime FRIDAY = OffsetDateTime.parse("2026-11-13T15:05:00+09:00");
    private static final OffsetDateTime MONDAY = OffsetDateTime.parse("2026-11-16T14:37:47+09:00");

    @Test
    void stateLeftFromThePreviousTradingDayIsNotReusedSoTheMoveStartsToday() {
        snapshot(FRIDAY, 10_300, up(DetectionType.PREV_CLOSE, 10_300, FRIDAY));

        snapshot(MONDAY, 10_900, up(DetectionType.PREV_CLOSE, 10_900, MONDAY));

        verify(publisher).publish(eq("TEST01"), any(Detection.class), eq(MONDAY), eq(MONDAY), anyString());
        assertThat(stored.get(DetectionType.PREV_CLOSE).moveStartedAt()).isEqualTo(MONDAY);
    }

    @Test
    void aMoveTodayNearThePriceLastPublishedOnThePreviousTradingDayIsStillPublished() {
        snapshot(FRIDAY, 10_300, up(DetectionType.PREV_CLOSE, 10_300, FRIDAY));

        // 지난 거래일에 알린 가격과 거의 같아도 오늘 처음 감지한 변동이므로 알린다.
        snapshot(MONDAY, 10_310, up(DetectionType.PREV_CLOSE, 10_310, MONDAY));

        verify(publisher).publish(eq("TEST01"), any(Detection.class), eq(MONDAY), eq(MONDAY), anyString());
    }

    @Test
    void aMoveFromThePreviousTradingDayThatClearedTodayDoesNotHoldBackTodaysMove() {
        snapshot(FRIDAY, 10_300, up(DetectionType.PREV_CLOSE, 10_300, FRIDAY));
        // 월요일 장 시작에 조건이 풀려 금요일 상태가 끝난 변동으로 표시된다.
        snapshot(MONDAY.minusMinutes(10), 10_000, Evaluation.clear(DetectionType.PREV_CLOSE));

        snapshot(MONDAY, 10_310, up(DetectionType.PREV_CLOSE, 10_310, MONDAY));

        verify(publisher).publish(eq("TEST01"), any(Detection.class), eq(MONDAY), eq(MONDAY), anyString());
    }

    @Test
    void aSnapshotFromAnEarlierDayDoesNotDropTodaysState() {
        snapshot(MONDAY, 10_900, up(DetectionType.PREV_CLOSE, 10_900, MONDAY));

        // 지난 시세를 나중에 재생해도 오늘 상태는 그대로다.
        snapshot(FRIDAY, 10_300, up(DetectionType.PREV_CLOSE, 10_300, FRIDAY));

        verify(states, never()).delete(anyString(), any(DetectionType.class));
        assertThat(stored.get(DetectionType.PREV_CLOSE).moveStartedAt()).isEqualTo(MONDAY);
    }

    @Test
    void aStreakCountedOnThePreviousTradingDayStartsOverToday() {
        MoveTracker target = confirming();
        snapshot(target, FRIDAY, 10_200, up(DetectionType.PREV_CLOSE, 10_200, FRIDAY));
        snapshot(target, FRIDAY.plusMinutes(1), 10_200, up(DetectionType.PREV_CLOSE, 10_200, FRIDAY.plusMinutes(1)));

        snapshot(target, MONDAY, 10_200, up(DetectionType.PREV_CLOSE, 10_200, MONDAY));
        verify(publisher, never()).publish(anyString(), any(), any(), any(), anyString());

        snapshot(target, MONDAY.plusSeconds(1), 10_200, up(DetectionType.PREV_CLOSE, 10_200, MONDAY.plusSeconds(1)));
        snapshot(target, MONDAY.plusSeconds(2), 10_200, up(DetectionType.PREV_CLOSE, 10_200, MONDAY.plusSeconds(2)));
        verify(publisher).publish(eq("TEST01"), any(Detection.class), eq(MONDAY), eq(MONDAY.plusSeconds(2)), anyString());
    }
}

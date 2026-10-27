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

class MoveTrackerTest {

    private static final OffsetDateTime T0 = OffsetDateTime.parse("2026-10-20T16:00:00+09:00");

    private MoveDetector detector;
    private MoveStateRepository states;
    private MovePublisher publisher;
    private MoveTracker tracker;
    // 저장소 대역이 기억하는 상태. 재시작은 새 MoveTracker를 같은 저장소에 붙여서 흉내 낸다.
    private final Map<DetectionType, MoveState> stored = new HashMap<>();

    @BeforeEach
    void setUp() {
        detector = mock(MoveDetector.class);
        states = mock(MoveStateRepository.class);
        publisher = mock(MovePublisher.class);
        when(states.find(anyString(), any(DetectionType.class)))
                .thenAnswer(inv -> Optional.ofNullable(stored.get(inv.<DetectionType>getArgument(1))));
        doAnswer(inv -> {
            stored.put(inv.getArgument(1), new MoveState(inv.getArgument(2), inv.getArgument(3)));
            return null;
        }).when(states).save(anyString(), any(DetectionType.class), anyLong(), any(OffsetDateTime.class), any(OffsetDateTime.class));
        doAnswer(inv -> {
            stored.remove(inv.<DetectionType>getArgument(1));
            return null;
        }).when(states).delete(anyString(), any(DetectionType.class));
        tracker = new MoveTracker(detector, states, publisher);
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
        assertThat(stored.get(DetectionType.WINDOW)).isEqualTo(new MoveState(10_200, start));
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
    void stateIsClearedWhenTheConditionEndsSoTheNextCrossingIsANewMove() {
        snapshot(T0, 10_200, window(10_200, T0));
        snapshot(T0.plusSeconds(1), 10_020, Evaluation.clear(DetectionType.WINDOW));
        assertThat(stored).doesNotContainKey(DetectionType.WINDOW);

        // 이전 발행 가격(10200)과 거의 같아도 새 변동이므로 바로 발행한다.
        OffsetDateTime again = T0.plusSeconds(2);
        snapshot(again, 10_210, window(10_210, again));

        verify(publisher, times(2)).publish(anyString(), any(), any(), any(), anyString());
        verify(publisher).publish(eq("TEST01"), any(), eq(again), eq(again), anyString());
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

        MoveTracker restarted = new MoveTracker(detector, states, publisher);
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
}

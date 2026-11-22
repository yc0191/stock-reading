package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.stockreading.market.MarketProperties;
import tools.jackson.databind.json.JsonMapper;

/**
 * 레벨 상(prevCloseHigh)을 실제 MoveDetector·MoveTracker에 가짜 시세를 받은 순서대로 넣어 확인한다.
 * 상태 저장소만 메모리 대역이고, 5분 비교(window)는 이력이 없어 늘 풀린 상태다. 가짜 종목·가짜 칸 이름이다.
 */
class MoveHighLevelTest {

    private static final OffsetDateTime DAY1 = OffsetDateTime.parse("2026-10-20T09:00:00+09:00");
    private static final String RAW = "{\"fake\":{\"a\":10000}}";

    private final Map<String, MoveState> stored = new HashMap<>();
    private final Map<String, LastPublish> lastPublishes = new HashMap<>();
    private final List<Detection> published = new ArrayList<>();
    private final List<OffsetDateTime> publishedStarts = new ArrayList<>();

    private MoveStateRepository states;
    private MovePublisher publisher;

    @BeforeEach
    void setUp() {
        states = mock(MoveStateRepository.class);
        publisher = mock(MovePublisher.class);
        when(states.find(anyString(), any(DetectionType.class))).thenAnswer(inv ->
                Optional.ofNullable(stored.get(key(inv.getArgument(0), inv.getArgument(1)))));
        doAnswer(inv -> {
            stored.put(key(inv.getArgument(0), inv.getArgument(1)),
                    new MoveState(inv.getArgument(2), inv.getArgument(3), inv.getArgument(4)));
            return null;
        }).when(states).save(anyString(), any(DetectionType.class), anyLong(), any(OffsetDateTime.class), any(OffsetDateTime.class));
        doAnswer(inv -> {
            stored.remove(key(inv.getArgument(0), inv.getArgument(1)));
            return null;
        }).when(states).delete(anyString(), any(DetectionType.class));
        doAnswer(inv -> {
            stored.computeIfPresent(key(inv.getArgument(0), inv.getArgument(1)),
                    (k, state) -> MoveTrackerTest.endedAt(state, inv.getArgument(2)));
            return null;
        }).when(states).markEnded(anyString(), any(DetectionType.class), any(OffsetDateTime.class));
        when(states.findLastPublish(anyString())).thenAnswer(inv ->
                Optional.ofNullable(lastPublishes.get(inv.<String>getArgument(0))));
        doAnswer(inv -> {
            lastPublishes.put(inv.getArgument(0), new LastPublish(inv.getArgument(1), inv.getArgument(2)));
            return null;
        }).when(states).saveLastPublish(anyString(), anyLong(), any(OffsetDateTime.class));
        doAnswer(inv -> {
            published.add(inv.getArgument(1));
            publishedStarts.add(inv.getArgument(2));
            return null;
        }).when(publisher).publish(anyString(), any(Detection.class), any(OffsetDateTime.class), any(OffsetDateTime.class), any());
    }

    private static String key(String symbol, DetectionType type) {
        return symbol + "/" + type;
    }

    private MoveTracker tracker(int confirmCount) {
        MarketProperties properties = new MarketProperties(5, new BigDecimal("3.00"), List.of("fake.a"), new BigDecimal("5.00"),
                List.of(), confirmCount, new BigDecimal("1.00"), List.of(), 120, new BigDecimal("3.00"), new BigDecimal("7.00"));
        PriceSnapshotRepository snapshots = mock(PriceSnapshotRepository.class);
        MoveDetector detector = new MoveDetector(snapshots, states, properties, JsonMapper.builder().build());
        return new MoveTracker(detector, states, publisher, properties);
    }

    private static void feed(MoveTracker tracker, String symbol, OffsetDateTime at, long price) {
        tracker.onSnapshot(symbol, at, price, RAW, "corr");
    }

    private List<Detection> highs() {
        return published.stream().filter(d -> d.type() == DetectionType.PREV_CLOSE_HIGH).toList();
    }

    @Test
    void highFollowsTheBasePriceAndIgnoresTheRepublishInterval() {
        MoveTracker tracker = tracker(1);

        feed(tracker, "TEST01", DAY1, 9_500);                      // 전일 종가 대비 -5.0%: 중만
        assertThat(published).extracting(Detection::type).containsExactly(DetectionType.PREV_CLOSE);

        feed(tracker, "TEST01", DAY1.plusMinutes(10), 9_280);      // -7.2%: 중 발행 10분 뒤라도 상
        assertThat(highs()).hasSize(1);
        Detection first = highs().get(0);
        assertThat(first.previousPrice()).isEqualTo(10_000L);
        assertThat(first.changePercent()).isEqualByComparingTo("-7.20");
        assertThat(first.todayChangePercent()).isEqualByComparingTo("-7.20");
        assertThat(first.thresholdPercent()).isEqualByComparingTo("7.00");
        assertThat(first.type().wire()).isEqualTo("prevCloseHigh");

        feed(tracker, "TEST01", DAY1.plusMinutes(20), 9_300);      // 기준가(9280) 대비 +0.2%: 상 아님
        feed(tracker, "TEST01", DAY1.plusMinutes(30), 9_930);      // 기준가 대비 +7.00%: 오늘 등락(-0.70%)은 ±7% 안이어도 상
        assertThat(highs()).hasSize(2);
        Detection rebound = highs().get(1);
        assertThat(rebound.previousPrice()).isEqualTo(9_280L);
        assertThat(rebound.changePercent()).isEqualByComparingTo("7.00");
        assertThat(rebound.todayChangePercent()).isEqualByComparingTo("-0.70");

        feed(tracker, "TEST01", DAY1.plusMinutes(40), 9_300);      // 새 기준가(9930) 대비 -6.34%: 상 아님
        assertThat(highs()).hasSize(2);
    }

    @Test
    void justBelowTheHighRatioFromTheBaseIsNotHigh() {
        MoveTracker tracker = tracker(1);

        feed(tracker, "TEST01", DAY1, 9_301);                      // -6.99%

        assertThat(highs()).isEmpty();
    }

    @Test
    void theBaseStartsOverFromThePreviousCloseTheNextDay() {
        MoveTracker tracker = tracker(1);
        feed(tracker, "TEST01", DAY1, 9_280);
        feed(tracker, "TEST01", DAY1.plusMinutes(30), 9_930);
        assertThat(highs()).hasSize(2);

        // 어제 기준가(9930) 그대로였다면 -6.34%라 나가지 않는다. 전일 종가(10000)부터 다시이므로 -7.00%에 발행한다.
        feed(tracker, "TEST01", DAY1.plusDays(1), 9_300);

        assertThat(highs()).hasSize(3);
        assertThat(highs().get(2).previousPrice()).isEqualTo(10_000L);
        assertThat(highs().get(2).changePercent()).isEqualByComparingTo("-7.00");
    }

    @Test
    void aGapOpenPublishesBothMiddleAndHighOnTheSameSnapshot() {
        MoveTracker tracker = tracker(1);

        feed(tracker, "TEST02", DAY1, 9_200);                      // 장 시작 갭 -8%

        assertThat(published).extracting(Detection::type)
                .containsExactly(DetectionType.PREV_CLOSE, DetectionType.PREV_CLOSE_HIGH);
        assertThat(highs().get(0).previousPrice()).isEqualTo(10_000L);
        assertThat(highs().get(0).changePercent()).isEqualByComparingTo("-8.00");
    }

    @Test
    void highUsesTheSameConfirmationAsTheOtherTypes() {
        MoveTracker tracker = tracker(3);

        feed(tracker, "TEST03", DAY1, 9_200);
        feed(tracker, "TEST03", DAY1.plusSeconds(1), 9_200);
        assertThat(highs()).isEmpty();
        feed(tracker, "TEST03", DAY1.plusSeconds(2), 10_000);      // 튄 값은 확인을 이어받지 못한다
        feed(tracker, "TEST03", DAY1.plusSeconds(3), 9_200);
        assertThat(highs()).isEmpty();

        feed(tracker, "TEST03", DAY1.plusSeconds(4), 9_200);
        feed(tracker, "TEST03", DAY1.plusSeconds(5), 9_200);

        assertThat(highs()).hasSize(1);
    }

    @Test
    void highWithoutAPreviousCloseAndWithoutAPublishedBaseIsNotJudged() {
        MoveTracker tracker = tracker(1);

        tracker.onSnapshot("TEST04", DAY1, 5_000, "{\"other\":1}", "corr");

        assertThat(published).isEmpty();
    }

    @Test
    void eachHighStartsANewMoveAtItsOwnTime() {
        MoveTracker tracker = tracker(1);
        feed(tracker, "TEST05", DAY1, 9_280);
        feed(tracker, "TEST05", DAY1.plusMinutes(30), 9_930);

        assertThat(publishedStarts.get(publishedStarts.size() - 1)).isEqualTo(DAY1.plusMinutes(30));
    }

    @Test
    void highDoesNotChangeTheLastPublishThatTheIntervalOfTheOtherTypesUses() {
        MoveTracker tracker = tracker(1);
        feed(tracker, "TEST06", DAY1, 9_500);                       // 중 발행(-5%), 마지막 발행 가격 9500
        feed(tracker, "TEST06", DAY1.plusMinutes(10), 9_280);       // 상 발행(-7.2%)

        assertThat(lastPublishes.get("TEST06").price()).isEqualTo(9_500L);

        // 상이 없을 때와 같다: 마지막 발행(9500) 대비 3% 미만(9300은 -2.1%)이면 간격 안에서 막는다.
        int before = published.size();
        feed(tracker, "TEST06", DAY1.plusMinutes(20), 9_300);
        assertThat(published.size()).isEqualTo(before);
    }

    @Test
    void aSecondHighRightAfterTheFirstStartsItsOwnStreak() {
        MoveTracker tracker = tracker(1);
        feed(tracker, "TEST07", DAY1, 9_280);                       // 상 발행
        feed(tracker, "TEST07", DAY1.plusMinutes(1), 8_600);        // 기준가(9280) 대비 -7.33%: 같은 방향으로 다시 상

        assertThat(highs()).hasSize(2);
        assertThat(publishedStarts.get(publishedStarts.size() - 1)).isEqualTo(DAY1.plusMinutes(1));
    }
}

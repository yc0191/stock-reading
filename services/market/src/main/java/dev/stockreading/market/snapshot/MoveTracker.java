package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.market.MarketProperties;

/**
 * 감지 결과를 market.move 발행으로 잇는다. 발행하려면 세 관문을 지난다.
 * 첫째, 변동 조건이 받은 스냅샷 기준으로 confirmCount건 연속 이어져야 한다(한 건만 튄 값은 발행하지 않는다).
 * 둘째, 종목·유형별로 한 번 발행한 뒤에는 마지막 발행 가격에서 임계값 이상 더 움직일 때만 재발행하고,
 * 감지 조건이 풀리면 상태를 지워 다음 변동을 처음부터 본다. 판단 불가(UNKNOWN) 스냅샷이 끼면 연속 건수를 다시 센다.
 * 셋째, 종목 단위로 마지막 발행(유형 무관) 뒤 재발행 간격 안이면 마지막 발행 가격 대비 예외 비율 이상 더 움직일 때만 발행한다.
 * 간격 때문에 막은 변동은 그 시점의 가격으로 유형 상태에 남겨, 조건이 풀리기 전에는 간격이 끝나도 새 움직임 없이 발행하지 않는다.
 */
@Component
class MoveTracker {

    private static final Logger log = LoggerFactory.getLogger(MoveTracker.class);

    private final MoveDetector detector;
    private final MoveStateRepository states;
    private final MovePublisher publisher;
    private final MarketProperties properties;
    // 연속 확인용 메모리 상태. 종목·유형 수만큼만 커지고, 재시작하면 비어서 연속 건수를 처음부터 센다.
    private final Map<String, Streak> streaks = new ConcurrentHashMap<>();

    MoveTracker(MoveDetector detector, MoveStateRepository states, MovePublisher publisher, MarketProperties properties) {
        this.detector = detector;
        this.states = states;
        this.publisher = publisher;
        this.properties = properties;
    }

    /** 같은 방향의 변동 조건이 이어진 건수. 방향이 바뀌면 새로 센다. */
    private static final class Streak {
        final int direction;
        final OffsetDateTime firstAsOf;
        int count = 1;
        boolean suppressionLogged;

        Streak(int direction, OffsetDateTime firstAsOf) {
            this.direction = direction;
            this.firstAsOf = firstAsOf;
        }
    }

    void onSnapshot(String symbol, OffsetDateTime asOf, long price, String raw, String correlationId) {
        for (Evaluation evaluation : detector.evaluate(symbol, asOf, price, raw)) {
            String key = symbol + "/" + evaluation.type();
            switch (evaluation.outcome()) {
                case CLEAR -> {
                    streaks.remove(key);
                    states.delete(symbol, evaluation.type());
                }
                // 판단할 수 없는 스냅샷이 끼면 연속이 아니므로 건수를 처음부터 다시 센다. 저장한 발행 상태는 그대로 둔다.
                case UNKNOWN -> streaks.remove(key);
                case DETECTED -> {
                    Streak streak = countStreak(key, asOf, evaluation.detection());
                    if (streak.count >= properties.confirmCount()) {
                        publishIfAllowed(symbol, asOf, price, evaluation.detection(), streak, correlationId);
                    }
                }
            }
        }
    }

    private Streak countStreak(String key, OffsetDateTime asOf, Detection detection) {
        int direction = detection.changePercent().signum();
        return streaks.compute(key, (k, streak) -> {
            if (streak == null || streak.direction != direction) {
                return new Streak(direction, asOf);
            }
            streak.count++;
            return streak;
        });
    }

    private void publishIfAllowed(
            String symbol, OffsetDateTime asOf, long price, Detection detection, Streak streak, String correlationId) {
        Optional<MoveState> state = states.find(symbol, detection.type());
        if (state.isPresent() && !movedFurther(state.get().lastPublishedPrice(), price, detection.thresholdPercent())) {
            log.info("{} {} move already published at {}, change since then is below {}%",
                    symbol, detection.type().wire(), state.get().lastPublishedPrice(), detection.thresholdPercent());
            return;
        }
        // 이미 시작된 변동이면 처음 시각을 유지한다. 새 변동이면 감지 규칙의 시각과 연속 확인이 시작된 스냅샷의 시각 중 이른 쪽이다.
        OffsetDateTime detectedStart = detection.moveStartedAt().isBefore(streak.firstAsOf)
                ? detection.moveStartedAt() : streak.firstAsOf;
        OffsetDateTime startedAt = state.map(MoveState::moveStartedAt).orElse(detectedStart);
        if (insideRepublishInterval(symbol, asOf, price, detection, streak)) {
            // 막은 변동도 이 유형의 상태로 남겨, 간격이 끝나는 순간 새 움직임 없이 발행되지 않게 한다(조건이 풀리면 지워진다).
            if (state.isEmpty()) {
                states.save(symbol, detection.type(), price, startedAt, asOf);
            }
            return;
        }
        // 발행한 뒤에 상태를 저장하므로 저장이 실패하면 중복은 있어도 누락은 없다.
        publisher.publish(symbol, detection, startedAt, asOf, correlationId);
        states.save(symbol, detection.type(), price, startedAt, asOf);
        states.saveLastPublish(symbol, price, asOf);
    }

    /** 간격 안이라 발행하지 않은 변동은 연속 확인이 이어지는 동안 한 번만 로그에 남긴다. */
    private boolean insideRepublishInterval(
            String symbol, OffsetDateTime asOf, long price, Detection detection, Streak streak) {
        int minutes = properties.republishIntervalMinutes();
        if (minutes <= 0) {
            return false;
        }
        Optional<LastPublish> last = states.findLastPublish(symbol);
        if (last.isEmpty() || !asOf.isBefore(last.get().publishedAt().plusMinutes(minutes))) {
            return false;
        }
        if (movedFurther(last.get().price(), price, properties.republishExceptionPercent())) {
            return false;
        }
        if (!streak.suppressionLogged) {
            streak.suppressionLogged = true;
            log.info("{} {} move not published: last move was published at {} within {} minutes, change since then is below {}%",
                    symbol, detection.type().wire(), last.get().publishedAt(), minutes, properties.republishExceptionPercent());
        }
        return true;
    }

    private static boolean movedFurther(long lastPublishedPrice, long price, BigDecimal threshold) {
        if (lastPublishedPrice <= 0) {
            return true;
        }
        return ChangeRate.percent(lastPublishedPrice, price).abs().compareTo(threshold) >= 0;
    }
}

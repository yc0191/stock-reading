package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.market.MarketProperties;

/**
 * 감지 결과를 market.move 발행으로 잇는다. 발행하려면 세 관문을 지난다.
 * 첫째, 변동 조건이 받은 스냅샷 기준으로 confirmCount건 연속 이어지고, 그 가격들이 허용 폭 안에 모여야 한다
 * (한 건만 튄 값은 조건이 이어지던 중이어도 발행하지 않는다).
 * 둘째, 종목·유형별로 한 번 발행한 뒤에는 마지막 발행 가격에서 임계값 이상 더 움직일 때만 재발행한다.
 * 감지 조건이 풀려도 그날은 마지막 발행 가격을 남겨, 기준선 언저리를 오르내리기만 한 종목을 다시 발행하지 않는다.
 * 풀린 뒤 다시 발행하는 변동은 새 변동으로 보고 시작 시각을 새로 잡는다. 판단 불가(UNKNOWN) 스냅샷이 끼면 연속 건수를 다시 센다.
 * 셋째, 종목 단위로 마지막 발행(유형 무관) 뒤 재발행 간격 안이면 마지막 발행 가격 대비 예외 비율 이상 더 움직일 때만 발행한다.
 * 간격 때문에 막은 변동은 그 시점의 가격으로 유형 상태에 남겨, 간격이 끝나도 새 움직임 없이 발행하지 않는다.
 * 레벨 상(prevCloseHigh)은 둘째·셋째 관문을 따로 쓴다. 상 기준가가 마지막 발행 가격이라 기준 비율 이상 움직인 것만 감지되므로
 * 재발행 판단이 이미 들어 있고, 종목별 재발행 간격은 적용하지 않는다. 상은 발행마다 새 변동으로 시작 시각을 잡고 연속 확인도 새로 센다.
 * 상은 종목별 마지막 발행 가격(재발행 간격 판단 기준)을 바꾸지 않으므로 다른 유형의 간격 판단은 상이 있어도 같다.
 * 연속 확인과 유형별 상태는 그날(한국 날짜) 것만 쓴다. 장이 끝나면 조건이 풀렸다는 판정이 오지 않아 상태가 다음 거래일까지 남는데,
 * 그대로 쓰면 다음 날 첫 변동의 시각과 근거 범위가 지난 거래일이 된다.
 */
@Component
class MoveTracker {

    private static final Logger log = LoggerFactory.getLogger(MoveTracker.class);
    private static final ZoneId ZONE = ZoneId.of("Asia/Seoul");

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
        // 마지막 confirmCount건의 가격. 조건만 세면 이어지던 변동에 튄 값 한 건이 끼어도 확인을 이어받으므로 가격이 모였는지도 본다.
        final Deque<Long> recentPrices = new ArrayDeque<>();
        int count = 1;
        boolean suppressionLogged;

        Streak(int direction, OffsetDateTime firstAsOf, long price) {
            this.direction = direction;
            this.firstAsOf = firstAsOf;
            recentPrices.addLast(price);
        }
    }

    void onSnapshot(String symbol, OffsetDateTime asOf, long price, String raw, String correlationId) {
        for (Evaluation evaluation : detector.evaluate(symbol, asOf, price, raw)) {
            String key = symbol + "/" + evaluation.type();
            switch (evaluation.outcome()) {
                case CLEAR -> {
                    streaks.remove(key);
                    states.markEnded(symbol, evaluation.type(), asOf);
                }
                // 판단할 수 없는 스냅샷이 끼면 연속이 아니므로 건수를 처음부터 다시 센다. 저장한 발행 상태는 그대로 둔다.
                case UNKNOWN -> streaks.remove(key);
                case DETECTED -> {
                    Streak streak = countStreak(key, asOf, price, evaluation.detection());
                    if (streak.count >= properties.confirmCount() && pricesAgree(streak)) {
                        publishIfAllowed(symbol, asOf, price, evaluation.detection(), streak, correlationId);
                    }
                }
            }
        }
    }

    private Streak countStreak(String key, OffsetDateTime asOf, long price, Detection detection) {
        int direction = detection.changePercent().signum();
        return streaks.compute(key, (k, streak) -> {
            if (streak == null || streak.direction != direction || fromEarlierDay(streak.firstAsOf, asOf)) {
                return new Streak(direction, asOf, price);
            }
            streak.count++;
            streak.recentPrices.addLast(price);
            while (streak.recentPrices.size() > Math.max(1, properties.confirmCount())) {
                streak.recentPrices.removeFirst();
            }
            return streak;
        });
    }

    /** 확인한 가격들이 가장 낮은 가격 대비 허용 폭 안에 모여 있는지. */
    private boolean pricesAgree(Streak streak) {
        long low = streak.recentPrices.stream().mapToLong(Long::longValue).min().orElse(0);
        long high = streak.recentPrices.stream().mapToLong(Long::longValue).max().orElse(0);
        if (low <= 0) {
            return true;
        }
        return ChangeRate.percent(low, high).compareTo(properties.confirmSpreadPercent()) <= 0;
    }

    private void publishIfAllowed(
            String symbol, OffsetDateTime asOf, long price, Detection detection, Streak streak, String correlationId) {
        Optional<MoveState> state = states.find(symbol, detection.type());
        // 끝난 변동은 시작 시각 칸에 풀린 시각이 들어 있으므로 지난 거래일 여부는 마지막 발행 시각으로 본다.
        if (state.isPresent() && fromEarlierDay(state.get().lastPublishedAt(), asOf)) {
            states.delete(symbol, detection.type());
            state = Optional.empty();
        }
        if (state.isPresent() && !movedFurther(state.get().lastPublishedPrice(), price, detection.thresholdPercent())) {
            log.info("{} {} move already published at {}, change since then is below {}%",
                    symbol, detection.type().wire(), state.get().lastPublishedPrice(), detection.thresholdPercent());
            return;
        }
        // 이미 시작된 변동이면 처음 시각을 유지한다. 새 변동이면 감지 규칙의 시각과 연속 확인이 시작된 스냅샷의 시각 중 이른 쪽이다.
        OffsetDateTime detectedStart = detection.moveStartedAt().isBefore(streak.firstAsOf)
                ? detection.moveStartedAt() : streak.firstAsOf;
        boolean high = detection.type() == DetectionType.PREV_CLOSE_HIGH;
        Optional<MoveState> ongoing = high ? Optional.empty() : state.filter(s -> !s.ended());
        OffsetDateTime startedAt = ongoing.map(MoveState::moveStartedAt).orElse(detectedStart);
        if (!high && insideRepublishInterval(symbol, asOf, price, detection, streak)) {
            // 막은 변동도 이 유형의 상태로 남겨, 간격이 끝나는 순간 새 움직임 없이 발행되지 않게 한다.
            if (ongoing.isEmpty()) {
                states.save(symbol, detection.type(), price, startedAt, asOf);
            }
            return;
        }
        // 발행한 뒤에 상태를 저장하므로 저장이 실패하면 중복은 있어도 누락은 없다.
        publisher.publish(symbol, detection, startedAt, asOf, correlationId);
        states.save(symbol, detection.type(), price, startedAt, asOf);
        if (high) {
            // 다음 상의 연속 확인과 시작 시각이 이번 상에서 이어받지 않고 새로 잡히게 한다.
            streaks.remove(symbol + "/" + detection.type());
        } else {
            states.saveLastPublish(symbol, price, asOf);
        }
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

    /** 지난 시세를 나중에 재생할 때 오늘 상태를 지우지 않도록, 이른 날짜만 지난 거래일 상태로 본다. */
    static boolean fromEarlierDay(OffsetDateTime stored, OffsetDateTime asOf) {
        return stored.atZoneSameInstant(ZONE).toLocalDate().isBefore(asOf.atZoneSameInstant(ZONE).toLocalDate());
    }

    private static boolean movedFurther(long lastPublishedPrice, long price, BigDecimal threshold) {
        if (lastPublishedPrice <= 0) {
            return true;
        }
        return ChangeRate.percent(lastPublishedPrice, price).abs().compareTo(threshold) >= 0;
    }
}

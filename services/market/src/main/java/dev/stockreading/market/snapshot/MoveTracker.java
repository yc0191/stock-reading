package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 감지 결과를 market.move 발행으로 잇는다. 한 번 발행한 뒤에는 종목·유형별로 마지막 발행 가격에서
 * 임계값 이상 더 움직일 때만 재발행하고, 감지 조건이 풀리면 상태를 지워 다음 변동을 처음부터 발행한다(ADR-019).
 */
@Component
class MoveTracker {

    private static final Logger log = LoggerFactory.getLogger(MoveTracker.class);

    private final MoveDetector detector;
    private final MoveStateRepository states;
    private final MovePublisher publisher;

    MoveTracker(MoveDetector detector, MoveStateRepository states, MovePublisher publisher) {
        this.detector = detector;
        this.states = states;
        this.publisher = publisher;
    }

    void onSnapshot(String symbol, OffsetDateTime asOf, long price, String raw, String correlationId) {
        for (Evaluation evaluation : detector.evaluate(symbol, asOf, price, raw)) {
            switch (evaluation.outcome()) {
                case CLEAR -> states.delete(symbol, evaluation.type());
                case UNKNOWN -> {
                }
                case DETECTED -> publishIfFurtherMove(symbol, asOf, price, evaluation.detection(), correlationId);
            }
        }
    }

    private void publishIfFurtherMove(
            String symbol, OffsetDateTime asOf, long price, Detection detection, String correlationId) {
        Optional<MoveState> state = states.find(symbol, detection.type());
        if (state.isPresent() && !movedFurther(state.get(), price, detection.thresholdPercent())) {
            log.info("{} {} move already published at {}, change since then is below {}%",
                    symbol, detection.type().wire(), state.get().lastPublishedPrice(), detection.thresholdPercent());
            return;
        }
        // 이미 시작된 변동이면 처음 시각을 유지한다. 발행한 뒤에 상태를 저장하므로 저장이 실패하면 중복은 있어도 누락은 없다.
        OffsetDateTime startedAt = state.map(MoveState::moveStartedAt).orElse(detection.moveStartedAt());
        publisher.publish(symbol, detection, startedAt, asOf, correlationId);
        states.save(symbol, detection.type(), price, startedAt, asOf);
    }

    private static boolean movedFurther(MoveState state, long price, BigDecimal threshold) {
        if (state.lastPublishedPrice() <= 0) {
            return true;
        }
        return ChangeRate.percent(state.lastPublishedPrice(), price).abs().compareTo(threshold) >= 0;
    }
}

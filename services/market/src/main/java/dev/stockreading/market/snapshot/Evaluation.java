package dev.stockreading.market.snapshot;

/**
 * 한 유형의 평가 결과.
 * DETECTED는 임계값 이상, CLEAR는 임계값 미만이거나 이력이 없어 변동이 이어지지 않는 경우,
 * UNKNOWN은 기준 값이 없거나 읽을 수 없어 판단하지 못한 경우다(재발행 상태를 건드리지 않는다).
 */
record Evaluation(DetectionType type, Outcome outcome, Detection detection) {

    enum Outcome {
        DETECTED, CLEAR, UNKNOWN
    }

    static Evaluation detected(Detection detection) {
        return new Evaluation(detection.type(), Outcome.DETECTED, detection);
    }

    static Evaluation clear(DetectionType type) {
        return new Evaluation(type, Outcome.CLEAR, null);
    }

    static Evaluation unknown(DetectionType type) {
        return new Evaluation(type, Outcome.UNKNOWN, null);
    }
}

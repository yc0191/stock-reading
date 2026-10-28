package dev.stockreading.market.snapshot;

/** 급변동 감지 유형. wire 값은 market.move 계약의 detectionType이다. */
enum DetectionType {
    WINDOW("window"),
    PREV_CLOSE("prevClose");

    private final String wire;

    DetectionType(String wire) {
        this.wire = wire;
    }

    String wire() {
        return wire;
    }
}

package dev.stockreading.market.context;

/** 배경 자료 종류와 저장 테이블의 대응. 테이블 이름은 이 열거형에서만 나오므로 SQL에 안전하게 붙인다. */
enum BackgroundKind {

    EXCHANGE_RATE_SUMMARY("exchange-rate-summary", "exchange_rate_summary", false),
    MARKET_SUMMARY("market-summary", "market_summary", false),
    WORLD_INDEX("world-index", "world_index", true),
    DAILY_PRICE("daily-price", "daily_price", true);

    private final String wireValue;
    private final String table;
    private final boolean hasTarget;

    BackgroundKind(String wireValue, String table, boolean hasTarget) {
        this.wireValue = wireValue;
        this.table = table;
        this.hasTarget = hasTarget;
    }

    String wireValue() {
        return wireValue;
    }

    String table() {
        return table;
    }

    boolean hasTarget() {
        return hasTarget;
    }
}

package dev.stockreading.market.insight;

/** payload.apiType 값과 저장 테이블의 대응. 테이블 이름은 이 열거형에서만 나오므로 SQL에 안전하게 붙인다. */
enum ApiType {

    DAILY_PRICE("daily-price", "daily_price", true),
    WORLD_INDEX("world-index", "world_index", true),
    EXCHANGE_RATE_SUMMARY("exchange-rate-summary", "exchange_rate_summary", false),
    MARKET_SUMMARY("market-summary", "market_summary", false);

    private final String wireValue;
    private final String table;
    private final boolean hasTarget;

    ApiType(String wireValue, String table, boolean hasTarget) {
        this.wireValue = wireValue;
        this.table = table;
        this.hasTarget = hasTarget;
    }

    String table() {
        return table;
    }

    boolean hasTarget() {
        return hasTarget;
    }

    static ApiType fromWire(String value) {
        for (ApiType type : values()) {
            if (type.wireValue.equals(value)) {
                return type;
            }
        }
        // 모르는 값은 조용히 버리지 않고 실패시켜 재시도 후 failed_event에 남긴다.
        throw new IllegalArgumentException("unknown apiType: " + value);
    }
}

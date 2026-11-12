package dev.stockreading.collector.marketdata;

/**
 * 1초 현재가 한 회차에서 부르는 종목 수. 회차마다 이만큼 호출하므로 초당 호출 상한을 넘으면 안 된다.
 * 상한 설정은 이 패키지 밖에서 읽지 못해(시세 제공자 설정), 검사를 여기서 한다.
 */
public record QuotePerTick(int value) {

    static QuotePerTick checked(int perTick, int maxCallsPerSecond) {
        if (perTick < 1 || perTick > maxCallsPerSecond) {
            throw new IllegalStateException("collector.quote-per-tick=" + perTick
                    + " must be between 1 and broker.max-calls-per-second=" + maxCallsPerSecond);
        }
        return new QuotePerTick(perTick);
    }
}

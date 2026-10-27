package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

class SimulatorMarketDataTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final List<String> SYMBOLS = List.of("TEST01", "TEST02", "TEST03");

    @Test
    void returnsOneQuotePerSymbolInOrder() {
        var quotes = new SimulatorMarketData(CLOCK).fetch(SYMBOLS).quotes();

        assertThat(quotes).extracting(PriceQuote::symbol).containsExactlyElementsOf(SYMBOLS);
    }

    @Test
    void stepsStayWithinConfiguredBoundAndAboveMinimum() {
        var simulator = new SimulatorMarketData(CLOCK);
        long previous = SimulatorMarketData.START_PRICE;

        for (int i = 0; i < 500; i++) {
            long price = simulator.fetch(List.of("TEST01")).quotes().get(0).price();
            double changePercent = Math.abs(price - previous) * 100.0 / previous;
            // 원 단위 반올림을 허용하려고 +0.01을 더한다.
            assertThat(changePercent).isLessThanOrEqualTo(SimulatorMarketData.MAX_STEP_PERCENT + 0.01);
            assertThat(price).isGreaterThanOrEqualTo(SimulatorMarketData.MIN_PRICE);
            previous = price;
        }
    }

    @Test
    void sameSeedProducesSameSequence() {
        var first = new SimulatorMarketData(CLOCK);
        var second = new SimulatorMarketData(CLOCK);

        for (int i = 0; i < 20; i++) {
            assertThat(first.fetch(SYMBOLS).quotes()).isEqualTo(second.fetch(SYMBOLS).quotes());
        }
    }
}

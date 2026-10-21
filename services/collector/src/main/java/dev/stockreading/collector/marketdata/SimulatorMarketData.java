package dev.stockreading.collector.marketdata;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/** 같은 결과가 재현되도록 시드를 고정한 랜덤 워크 가격 생성기. */
@Component
public class SimulatorMarketData implements MarketDataPort {

    static final long START_PRICE = 10_000L;
    static final long MIN_PRICE = 100L;
    static final double MAX_STEP_PERCENT = 1.5;

    private final Clock clock;
    private final Random random = new Random(42);
    private final Map<String, Long> lastPrice = new ConcurrentHashMap<>();

    public SimulatorMarketData(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String sourceName() {
        return "simulator";
    }

    @Override
    public List<PriceQuote> fetch(List<String> symbols) {
        OffsetDateTime now = OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        return symbols.stream().map(symbol -> {
            long previous = lastPrice.getOrDefault(symbol, START_PRICE);
            double stepPercent = (random.nextDouble() * 2 - 1) * MAX_STEP_PERCENT;
            long next = Math.max(MIN_PRICE, Math.round(previous * (1 + stepPercent / 100.0)));
            lastPrice.put(symbol, next);
            return new PriceQuote(symbol, next, now);
        }).toList();
    }
}

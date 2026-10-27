package dev.stockreading.collector.marketdata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.CollectorProperties;

@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
class BrokerMarketData implements MarketDataPort {

    /** 상한 때문에 호출하지 못한 종목을 TICK_SKIPPED 와 구분하는 exceptionType 값. 계약에 정식 값이 생기면 대체한다. */
    static final String RATE_LIMITED = "RateLimited";
    // tick 시각이 조금 어긋나도 곧 자리가 나는 호출은 기다렸다 보낸다.
    private static final Duration LIMITER_WAIT = Duration.ofMillis(300);
    private static final Logger log = LoggerFactory.getLogger(BrokerMarketData.class);

    private final BrokerClient client;
    private final CallRateLimiter limiter;
    private final int maxCallsPerSecond;
    private final AtomicInteger inFlight = new AtomicInteger();

    BrokerMarketData(BrokerClient client, CollectorProperties collector, BrokerProperties broker) {
        this.client = client;
        this.maxCallsPerSecond = broker.maxCallsPerSecond();
        this.limiter = new CallRateLimiter(maxCallsPerSecond, Duration.ofSeconds(1), System::nanoTime);
        int symbolCount = collector.symbols() == null ? 0 : collector.symbols().size();
        if (symbolCount > maxCallsPerSecond) {
            log.warn("symbols={} exceed max calls per second={}; the excess symbols are recorded as skipped every tick",
                    symbolCount, maxCallsPerSecond);
        }
    }

    @Override
    public String sourceName() {
        return "broker";
    }

    // 종목마다 따로 호출해 한 종목의 실패가 다른 종목에 번지지 않는다. 재시도하지 않고 다음 tick에 맡긴다.
    @Override
    public FetchResult fetch(List<String> symbols) {
        int callable = Math.min(symbols.size(), maxCallsPerSecond);
        List<Future<Outcome>> futures = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (String symbol : symbols.subList(0, callable)) {
                futures.add(executor.submit(() -> call(symbol)));
            }
        }
        List<PriceQuote> quotes = new ArrayList<>();
        List<QuoteFailure> failures = new ArrayList<>();
        for (int i = 0; i < futures.size(); i++) {
            Outcome outcome = result(symbols.get(i), futures.get(i));
            if (outcome.quote() != null) {
                quotes.add(outcome.quote());
            } else {
                failures.add(outcome.failure());
            }
        }
        for (String symbol : symbols.subList(callable, symbols.size())) {
            failures.add(QuoteFailure.skipped(symbol, RATE_LIMITED));
        }
        return new FetchResult(quotes, failures);
    }

    private record Outcome(PriceQuote quote, QuoteFailure failure) {
    }

    private static Outcome result(String symbol, Future<Outcome> future) {
        try {
            return future.get();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return new Outcome(null, new QuoteFailure(symbol, FailureType.CONNECTION_FAILED, null, null, null,
                    e.getClass().getSimpleName(), null, null, null, null));
        }
    }

    private Outcome call(String symbol) {
        if (!limiter.tryAcquire(LIMITER_WAIT)) {
            return new Outcome(null, QuoteFailure.skipped(symbol, RATE_LIMITED));
        }
        int concurrent = inFlight.incrementAndGet();
        long start = System.nanoTime();
        try {
            return new Outcome(client.currentPrice(symbol), null);
        } catch (BrokerApiException e) {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            // 서버가 보낸 문구는 로그에 남기지 않는다. 메시지는 우리가 만든 문구와 처리 코드뿐이다.
            log.warn("quote failed symbol={} type={} reason={}", symbol, e.failureType(), e.getMessage());
            BrokerClient.TokenState token = client.tokenState();
            return new Outcome(null, new QuoteFailure(symbol, e.failureType(), e.httpStatus(), e.responseCode(),
                    elapsedMs, e.exceptionType(),
                    token == null ? null : token.ageMs(), token == null ? null : token.remainingMs(),
                    inFlight.get(), e.responseRaw()));
        } catch (RuntimeException e) {
            // BrokerClient 가 예상하지 못한 오류. 메시지는 남기지 않고 예외 종류만 기록한다.
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            log.warn("quote failed symbol={} exception={}", symbol, e.getClass().getSimpleName());
            return new Outcome(null, new QuoteFailure(symbol, FailureType.RESULT_CODE_FAILED, null, null, elapsedMs,
                    e.getClass().getSimpleName(), null, null, concurrent, null));
        } finally {
            inFlight.decrementAndGet();
        }
    }
}

package dev.stockreading.collector.schedule;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.event.FailurePayload;
import dev.stockreading.collector.event.SnapshotPayload;
import dev.stockreading.collector.holdings.QuoteSymbolsPort;
import dev.stockreading.collector.marketdata.FetchResult;
import dev.stockreading.collector.marketdata.MarketDataPort;
import dev.stockreading.collector.marketdata.PriceQuote;
import dev.stockreading.collector.marketdata.QuoteFailure;
import dev.stockreading.collector.marketdata.QuotePerTick;
import tools.jackson.databind.ObjectMapper;

/**
 * 1초 현재가 한 회차: 회차마다 목록을 돌며 회차당 수만큼의 종목 시세를 조회해 종목마다 market.snapshot 을,
 * 실패와 건너뜀은 market.snapshot.failed 를 발행한다. 차례가 아닌 종목은 아무것도 남기지 않는다.
 * 앞 회차 조회가 끝나지 않아 건너뛴 회차는 조회 없이 그 회차 차례 종목만 건너뜀으로 남기고 차례는 넘기지 않는다.
 */
@Component
class QuotePublisher {

    private static final Logger log = LoggerFactory.getLogger(QuotePublisher.class);
    /** 앞 회차 조회가 끝나지 않아 조회하지 않은 회차를 TICK_SKIPPED 의 다른 사유(RateLimited 등)와 구분하는 exceptionType 값. */
    static final String SUPERSEDED = "SupersededByNewerTick";

    /** 시세 종목이 비었다는 경고는 1초 회차마다 쏟아지지 않게 이 간격으로만 남긴다. */
    static final Duration EMPTY_WARN_INTERVAL = Duration.ofMinutes(10);

    private final MarketDataPort marketData;
    private final QuoteSymbolsPort quoteSymbols;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final int quotePerTick;
    private Instant lastEmptyWarnAt;
    // 다음 회차가 부를 첫 종목의 위치. 종목 목록이 바뀌어도 나머지 연산으로 맞춘다.
    private int cursor;

    QuotePublisher(MarketDataPort marketData, QuoteSymbolsPort quoteSymbols,
                   KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock,
                   CollectorProperties collector, QuotePerTick perTick) {
        this.marketData = marketData;
        this.quoteSymbols = quoteSymbols;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
        this.quotePerTick = perTick.value();
        int symbolCount = quoteSymbols.symbols().size();
        if (symbolCount > 0) {
            long rounds = (symbolCount + quotePerTick - 1L) / quotePerTick;
            log.info("quote rotation symbols={} perTick={} each symbol is fetched about every {} ms",
                    symbolCount, quotePerTick, rounds * collector.tickIntervalMs());
        }
    }

    void publishCollected(String correlationId, String scheduledFor) {
        warnIfEmpty();
        FetchResult result = marketData.fetch(takeTurn(true));
        for (PriceQuote quote : result.quotes()) {
            Envelope<SnapshotPayload> snapshot = EventFactory.create(
                    clock,
                    "market.snapshot",
                    quote.asOf(),
                    correlationId,
                    new SnapshotPayload(
                            quote.symbol(), quote.price(), EventFactory.format(quote.asOf()), marketData.sourceName(),
                            quote.raw()));
            send(Topics.MARKET_SNAPSHOT, quote.symbol(), snapshot);
        }
        publishFailures(result.failures(), correlationId, scheduledFor, result.quotes().size());
        log.info("published {} market.snapshot, {} market.snapshot.failed events corr={}",
                result.quotes().size(), result.failures().size(), correlationId);
    }

    // 회차는 한 스레드에서 차례로 돌지만 수동 실행과 겹칠 수 있어 잠근다.
    private synchronized void warnIfEmpty() {
        if (!quoteSymbols.symbols().isEmpty()) {
            return;
        }
        Instant now = clock.instant();
        if (lastEmptyWarnAt == null || !now.isBefore(lastEmptyWarnAt.plus(EMPTY_WARN_INTERVAL))) {
            lastEmptyWarnAt = now;
            log.warn("quote symbols are empty (personal.collect_target); load the seed data and restart the collector");
        }
    }

    // advance 가 거짓이면 차례를 넘기지 않아, 건너뛴 회차의 종목을 다음 회차가 그대로 부른다.
    private synchronized List<String> takeTurn(boolean advance) {
        List<String> symbols = quoteSymbols.symbols();
        int size = symbols.size();
        if (size == 0) {
            return List.of();
        }
        int count = Math.min(quotePerTick, size);
        int start = cursor % size;
        List<String> turn = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            turn.add(symbols.get((start + i) % size));
        }
        if (advance) {
            cursor = (start + count) % size;
        }
        return turn;
    }

    void publishSkipped(String correlationId, String scheduledFor) {
        List<QuoteFailure> skipped = takeTurn(false).stream()
                .map(symbol -> QuoteFailure.skipped(symbol, SUPERSEDED)).toList();
        publishFailures(skipped, correlationId, scheduledFor, 0);
    }

    private void publishFailures(List<QuoteFailure> failures, String correlationId, String scheduledFor, int succeeded) {
        for (QuoteFailure failure : failures) {
            Envelope<FailurePayload> event = EventFactory.create(
                    clock,
                    "market.snapshot.failed",
                    OffsetDateTime.now(clock),
                    correlationId,
                    new FailurePayload(
                            failure.symbol(),
                            failure.type().name(),
                            scheduledFor,
                            failure.httpStatus(),
                            failure.responseCode(),
                            failure.elapsedMs(),
                            failure.exceptionType(),
                            failure.tokenAgeMs() == null ? null
                                    : new FailurePayload.TokenState(failure.tokenAgeMs(), failure.tokenRemainingMs()),
                            failure.concurrentCalls(),
                            succeeded,
                            failures.size(),
                            failure.responseRaw()));
            send(Topics.MARKET_SNAPSHOT_FAILED, failure.symbol(), event);
        }
    }

    // 한 건의 발행 실패가 다른 종목의 발행을 막지 않게 한다. 본문에는 외부 원문이 있을 수 있어 예외 종류만 남긴다.
    private void send(String topic, String key, Object event) {
        try {
            kafka.send(topic, key, mapper.writeValueAsString(event));
        } catch (RuntimeException e) {
            log.error("publish failed topic={} key={} exception={}", topic, key, e.getClass().getSimpleName());
        }
    }
}

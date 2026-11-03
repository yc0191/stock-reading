package dev.stockreading.collector.schedule;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.event.FailurePayload;
import dev.stockreading.collector.event.SnapshotPayload;
import dev.stockreading.collector.event.TickEnvelope;
import dev.stockreading.collector.marketdata.FetchResult;
import dev.stockreading.collector.marketdata.MarketDataPort;
import dev.stockreading.collector.marketdata.PriceQuote;
import dev.stockreading.collector.marketdata.QuoteFailure;
import tools.jackson.databind.ObjectMapper;

@Component
class TickListener {

    private static final Logger log = LoggerFactory.getLogger(TickListener.class);
    private static final int MAX_REMEMBERED_TICKS = 1_000;

    private final MarketDataPort marketData;
    private final CollectorProperties properties;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    // v0: 메모리 기반 중복 방지. 이후 DB 기록으로 대체한다.
    private final Set<String> processedTicks = ConcurrentHashMap.newKeySet();

    TickListener(MarketDataPort marketData, CollectorProperties properties,
                 KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.marketData = marketData;
        this.properties = properties;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @KafkaListener(topics = Topics.SCHEDULE_TICK, groupId = "collector")
    void onTick(String message) {
        TickEnvelope tick = mapper.readValue(message, TickEnvelope.class);
        String jobType = tick.payload().jobType();
        if (!TickPublisher.JOB_COLLECT.equals(jobType)) {
            log.debug("ignoring tick with jobType={}", jobType);
            return;
        }
        String dedupeKey = jobType + "|" + tick.payload().scheduledFor();
        if (!processedTicks.add(dedupeKey)) {
            log.info("duplicate tick ignored key={}", dedupeKey);
            return;
        }
        if (processedTicks.size() > MAX_REMEMBERED_TICKS) {
            processedTicks.clear();
            processedTicks.add(dedupeKey);
        }

        List<String> symbols = properties.symbols();
        OffsetDateTime scheduledFor = OffsetDateTime.parse(tick.payload().scheduledFor());
        Duration lag = Duration.between(scheduledFor, OffsetDateTime.now(clock));
        if (lag.toMillis() >= properties.maxTickAgeMs()) {
            // 밀린 tick 을 뒤늦게 처리하면 지금 시세가 과거 시각의 것처럼 기록된다. 건너뛰고 사실만 남긴다.
            log.warn("stale tick skipped lagMs={} corr={}", lag.toMillis(), tick.correlationId());
            List<QuoteFailure> skipped = symbols.stream().map(symbol -> QuoteFailure.skipped(symbol, null)).toList();
            publishFailures(skipped, tick, 0);
            return;
        }

        FetchResult result = marketData.fetch(symbols);
        for (PriceQuote quote : result.quotes()) {
            Envelope<SnapshotPayload> snapshot = EventFactory.create(
                    clock,
                    "market.snapshot",
                    quote.asOf(),
                    tick.correlationId(),
                    new SnapshotPayload(
                            quote.symbol(), quote.price(), EventFactory.format(quote.asOf()), marketData.sourceName(),
                            quote.raw()));
            send(Topics.MARKET_SNAPSHOT, quote.symbol(), snapshot);
        }
        publishFailures(result.failures(), tick, result.quotes().size());
        log.info("published {} market.snapshot, {} market.snapshot.failed events corr={}",
                result.quotes().size(), result.failures().size(), tick.correlationId());
    }

    private void publishFailures(List<QuoteFailure> failures, TickEnvelope tick, int succeeded) {
        for (QuoteFailure failure : failures) {
            Envelope<FailurePayload> event = EventFactory.create(
                    clock,
                    "market.snapshot.failed",
                    OffsetDateTime.now(clock),
                    tick.correlationId(),
                    new FailurePayload(
                            failure.symbol(),
                            failure.type().name(),
                            tick.payload().scheduledFor(),
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

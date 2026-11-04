package dev.stockreading.collector.schedule;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
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
    /** 더 최근 tick 이 함께 쌓여 있어 조회하지 않은 tick 을 TICK_SKIPPED 의 다른 사유(RateLimited 등)와 구분하는 exceptionType 값. */
    static final String SUPERSEDED = "SupersededByNewerTick";

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

    /**
     * 한 번에 받은 tick 묶음을 처리한다. 앞 tick 처리가 느려지면 그동안 발행된 tick 이 쌓였다가 다음 poll 에 함께 온다.
     * 쌓인 tick 을 모두 조회하면 짧은 시간에 호출이 몰려 호출 상한에 걸리므로, 가장 최근 tick 하나만 조회한다.
     */
    @KafkaListener(topics = Topics.SCHEDULE_TICK, groupId = "collector", batch = "true")
    void onTicks(List<String> messages) {
        List<TickEnvelope> candidates = new ArrayList<>();
        for (String message : messages) {
            TickEnvelope tick = accept(message);
            if (tick != null) {
                candidates.add(tick);
            }
        }
        if (candidates.isEmpty()) {
            return;
        }
        TickEnvelope latest = candidates.stream().max(Comparator.comparing(TickListener::scheduledFor)).orElseThrow();
        for (TickEnvelope older : candidates) {
            if (older != latest) {
                log.warn("superseded tick skipped scheduledFor={} latest={} corr={}",
                        older.payload().scheduledFor(), latest.payload().scheduledFor(), older.correlationId());
                publishSkipped(older, SUPERSEDED);
            }
        }
        fetchAndPublish(latest);
    }

    void onTick(String message) {
        onTicks(List.of(message));
    }

    /** 조회 후보인 tick 을 돌려준다. 다른 작업, 중복, 너무 오래된 tick 은 여기서 처리를 끝내고 null 을 돌려준다. */
    private TickEnvelope accept(String message) {
        TickEnvelope tick;
        try {
            tick = mapper.readValue(message, TickEnvelope.class);
        } catch (RuntimeException e) {
            // 묶음의 한 건이 깨져도 나머지 tick 은 처리한다. 본문은 남기지 않는다.
            log.error("unreadable tick ignored exception={}", e.getClass().getSimpleName());
            return null;
        }
        String jobType = tick.payload().jobType();
        if (!TickPublisher.JOB_COLLECT.equals(jobType)) {
            log.debug("ignoring tick with jobType={}", jobType);
            return null;
        }
        String dedupeKey = jobType + "|" + tick.payload().scheduledFor();
        if (!processedTicks.add(dedupeKey)) {
            log.info("duplicate tick ignored key={}", dedupeKey);
            return null;
        }
        if (processedTicks.size() > MAX_REMEMBERED_TICKS) {
            processedTicks.clear();
            processedTicks.add(dedupeKey);
        }

        Duration lag = Duration.between(scheduledFor(tick), OffsetDateTime.now(clock));
        if (lag.toMillis() >= properties.maxTickAgeMs()) {
            // 밀린 tick 을 뒤늦게 처리하면 지금 시세가 과거 시각의 것처럼 기록된다. 건너뛰고 사실만 남긴다.
            log.warn("stale tick skipped lagMs={} corr={}", lag.toMillis(), tick.correlationId());
            publishSkipped(tick, null);
            return null;
        }
        return tick;
    }

    private static OffsetDateTime scheduledFor(TickEnvelope tick) {
        return OffsetDateTime.parse(tick.payload().scheduledFor());
    }

    private void publishSkipped(TickEnvelope tick, String reason) {
        List<QuoteFailure> skipped = properties.symbols().stream()
                .map(symbol -> QuoteFailure.skipped(symbol, reason)).toList();
        publishFailures(skipped, tick, 0);
    }

    private void fetchAndPublish(TickEnvelope tick) {
        FetchResult result = marketData.fetch(properties.symbols());
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

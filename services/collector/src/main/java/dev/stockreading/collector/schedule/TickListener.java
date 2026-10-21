package dev.stockreading.collector.schedule;

import java.time.Clock;
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
import dev.stockreading.collector.event.SnapshotPayload;
import dev.stockreading.collector.event.TickEnvelope;
import dev.stockreading.collector.marketdata.MarketDataPort;
import dev.stockreading.collector.marketdata.PriceQuote;
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

    // v0: 메모리 기반 중복 방지. 이후 DB 기록으로 대체한다(ADR-012).
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

        List<PriceQuote> quotes = marketData.fetch(properties.symbols());
        for (PriceQuote quote : quotes) {
            Envelope<SnapshotPayload> snapshot = EventFactory.create(
                    clock,
                    "market.snapshot",
                    quote.asOf(),
                    tick.correlationId(),
                    new SnapshotPayload(
                            quote.symbol(), quote.price(), EventFactory.format(quote.asOf()), marketData.sourceName()));
            kafka.send(Topics.MARKET_SNAPSHOT, quote.symbol(), mapper.writeValueAsString(snapshot));
        }
        log.info("published {} market.snapshot events corr={}", quotes.size(), tick.correlationId());
    }
}

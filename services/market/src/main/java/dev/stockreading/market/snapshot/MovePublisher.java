package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.market.MarketProperties;
import dev.stockreading.market.Topics;
import dev.stockreading.market.event.Envelope;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.event.MovePayload;
import tools.jackson.databind.ObjectMapper;

@Component
class MovePublisher {

    private static final Logger log = LoggerFactory.getLogger(MovePublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final MarketProperties properties;
    private final Clock clock;

    MovePublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, MarketProperties properties, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.properties = properties;
        this.clock = clock;
    }

    /** @return market.move를 발행했으면 true. */
    boolean publishIfSignificant(
            String symbol, long previousPrice, long price, BigDecimal changePercent,
            OffsetDateTime asOf, String correlationId) {
        BigDecimal threshold = properties.moveThresholdPercent();
        if (changePercent.abs().compareTo(threshold) < 0) {
            return false;
        }
        Envelope<MovePayload> event = EventFactory.create(
                clock, "market.move", asOf, correlationId,
                new MovePayload(symbol, previousPrice, price, changePercent, threshold, EventFactory.format(asOf)));
        kafka.send(Topics.MARKET_MOVE, symbol, mapper.writeValueAsString(event));
        log.info("{} move detected change={}% (threshold {}%) corr={}", symbol, changePercent, threshold, correlationId);
        return true;
    }
}

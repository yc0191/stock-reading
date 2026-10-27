package dev.stockreading.market.snapshot;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import tools.jackson.databind.ObjectMapper;

@Component
class SnapshotListener {

    private static final Logger log = LoggerFactory.getLogger(SnapshotListener.class);

    private final PriceSnapshotRepository repository;
    private final PricePartitions partitions;
    private final MovePublisher movePublisher;
    private final ObjectMapper mapper;

    SnapshotListener(PriceSnapshotRepository repository, PricePartitions partitions,
            MovePublisher movePublisher, ObjectMapper mapper) {
        this.repository = repository;
        this.partitions = partitions;
        this.movePublisher = movePublisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.MARKET_SNAPSHOT, groupId = "market")
    void onSnapshot(String message) {
        SnapshotEnvelope event = mapper.readValue(message, SnapshotEnvelope.class);
        SnapshotEnvelope.SnapshotPayload payload = event.payload();
        OffsetDateTime asOf = OffsetDateTime.parse(payload.asOf());

        partitions.ensure(asOf);
        boolean inserted = repository.insertIfAbsent(
                UUID.fromString(event.eventId()), payload.symbol(), asOf, payload.price(),
                RawJson.toJsonb(mapper, payload.raw()));
        if (!inserted) {
            log.info("duplicate snapshot ignored eventId={}", event.eventId());
            return;
        }

        repository.findPreviousPrice(payload.symbol(), asOf).ifPresentOrElse(
                previous -> {
                    BigDecimal change = ChangeRate.percent(previous, payload.price());
                    log.info("{} price={} change={}% corr={}",
                            payload.symbol(), payload.price(), change, event.correlationId());
                    movePublisher.publishIfSignificant(
                            payload.symbol(), previous, payload.price(), change, asOf, event.correlationId());
                },
                () -> log.info("{} first snapshot price={} corr={}",
                        payload.symbol(), payload.price(), event.correlationId()));
    }
}

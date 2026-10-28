package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import tools.jackson.databind.ObjectMapper;

/** 시세 조회 실패 이벤트를 기록한다. 이 기록은 원인 분석용이며 다른 이벤트를 만들지 않는다. */
@Component
class QuoteFailureListener {

    private static final Logger log = LoggerFactory.getLogger(QuoteFailureListener.class);

    private final QuoteFailureRepository repository;
    private final ObjectMapper mapper;

    QuoteFailureListener(QuoteFailureRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.MARKET_SNAPSHOT_FAILED, groupId = "market-failure")
    void onFailure(String message) {
        FailureEnvelope event = mapper.readValue(message, FailureEnvelope.class);
        FailureEnvelope.FailurePayload payload = event.payload();

        boolean inserted = repository.insertIfAbsent(
                UUID.fromString(event.eventId()),
                event.correlationId(),
                OffsetDateTime.parse(event.occurredAt()),
                payload,
                OffsetDateTime.parse(payload.scheduledFor()),
                RawJson.toJsonb(mapper, payload.responseRaw()));
        if (!inserted) {
            log.info("duplicate quote failure ignored eventId={}", event.eventId());
            return;
        }
        log.info("{} quote failure type={} corr={}", payload.symbol(), payload.failureType(), event.correlationId());
    }
}

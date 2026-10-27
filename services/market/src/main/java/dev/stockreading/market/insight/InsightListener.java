package dev.stockreading.market.insight;

import java.time.OffsetDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import tools.jackson.databind.ObjectMapper;

/** 장 마감 후 조회 응답을 해석 없이 저장만 한다. 계산과 이벤트 발행은 하지 않는다. */
@Component
class InsightListener {

    static final String CONTAINER_FACTORY = "insightListenerContainerFactory";

    private static final Logger log = LoggerFactory.getLogger(InsightListener.class);

    private final InsightRepository repository;
    private final ObjectMapper mapper;

    InsightListener(InsightRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.MARKET_INSIGHT, groupId = "market-insight", containerFactory = CONTAINER_FACTORY)
    void onInsight(String message) {
        InsightEnvelope event = mapper.readValue(message, InsightEnvelope.class);
        InsightEnvelope.InsightPayload payload = event.payload();
        ApiType type = ApiType.fromWire(payload.apiType());

        boolean inserted = repository.insertIfAbsent(
                type,
                UUID.fromString(event.eventId()),
                payload.target(),
                payload.response(),
                OffsetDateTime.parse(payload.fetchedAt()));
        // 응답 본문은 로그에 남기지 않고 식별자만 남긴다.
        log.info("insight {} apiType={} eventId={} corr={}",
                inserted ? "stored" : "duplicate ignored", payload.apiType(), event.eventId(), event.correlationId());
    }
}

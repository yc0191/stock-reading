package dev.stockreading.market.snapshot;

import java.time.Clock;
import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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
    private final Clock clock;

    MovePublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * market.move를 발행한다. payload의 asOf는 변동 시각(moveStartedAt)이고, 이벤트의 occurredAt은
     * 변동을 감지한 스냅샷의 시각(observedAt)이다.
     */
    void publish(String symbol, Detection detection, OffsetDateTime moveStartedAt, OffsetDateTime observedAt, String correlationId) {
        String startedAt = EventFactory.format(moveStartedAt);
        Envelope<MovePayload> event = EventFactory.create(
                clock, "market.move", observedAt, correlationId,
                new MovePayload(symbol, detection.previousPrice(), detection.price(), detection.changePercent(),
                        detection.thresholdPercent(), startedAt, detection.type().wire(), startedAt,
                        detection.todayChangePercent()));
        kafka.send(Topics.MARKET_MOVE, symbol, mapper.writeValueAsString(event));
        log.info("{} {} move detected change={}% today={}% (threshold {}%) since {} corr={}",
                symbol, detection.type().wire(), detection.changePercent(), detection.todayChangePercent(),
                detection.thresholdPercent(), startedAt, correlationId);
    }
}

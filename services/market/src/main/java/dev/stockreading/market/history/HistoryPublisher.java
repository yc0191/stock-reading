package dev.stockreading.market.history;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.Envelope;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.event.HistoryPayload;
import tools.jackson.databind.ObjectMapper;

@Component
class HistoryPublisher {

    private static final Logger log = LoggerFactory.getLogger(HistoryPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    HistoryPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 요청의 jobId, seq, correlationId를 그대로 이어받아 응답을 발행한다. 메시지 키는 종목코드다. */
    void publish(String jobId, int seq, String symbol, List<HistoryPayload.Point> points,
            boolean truncated, String correlationId) {
        Envelope<HistoryPayload> event = EventFactory.create(
                clock, "market.history.responded", OffsetDateTime.now(clock), correlationId,
                new HistoryPayload(jobId, seq, symbol, points, truncated));
        kafka.send(Topics.MARKET_HISTORY_RESPONDED, symbol, mapper.writeValueAsString(event));
        log.info("{} history responded jobId={} seq={} points={} truncated={} corr={}",
                symbol, jobId, seq, points.size(), truncated, correlationId);
    }
}

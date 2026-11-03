package dev.stockreading.market.context;

import java.time.Clock;
import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.Envelope;
import dev.stockreading.market.event.EventFactory;
import tools.jackson.databind.ObjectMapper;

@Component
class ContextPublisher {

    private static final Logger log = LoggerFactory.getLogger(ContextPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    ContextPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 요청의 correlationId를 이어받아 응답을 발행한다. jobId·seq는 payload에 이미 요청 값으로 들어 있다. 메시지 키는 종목코드다. */
    void publish(ContextPayload payload, String requestEventId, String correlationId) {
        Envelope<ContextPayload> event = EventFactory.create(
                clock, Topics.MARKET_CONTEXT_RESPONDED, OffsetDateTime.now(clock), correlationId, payload);
        kafka.send(Topics.MARKET_CONTEXT_RESPONDED, payload.symbol(), mapper.writeValueAsString(event));
        // 종목코드·티커·원문은 보유 종목과 자료 내용이 드러나므로 식별자와 건수만 남긴다.
        long ok = payload.background().stream().filter(b -> "ok".equals(b.status())).count();
        long tooLarge = payload.background().stream().filter(b -> "too-large".equals(b.status())).count();
        log.info("context responded jobId={} seq={} requestEventId={} tickers={} closeRates={} truncated={} "
                        + "background ok={} tooLarge={} missing={} corr={}",
                payload.jobId(), payload.seq(), requestEventId, payload.tickers().size(), payload.closeRates().size(),
                payload.truncated(), ok, tooLarge, payload.background().size() - ok - tooLarge, correlationId);
    }
}

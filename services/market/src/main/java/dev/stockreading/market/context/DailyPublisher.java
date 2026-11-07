package dev.stockreading.market.context;

import java.time.Clock;
import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.Envelope;
import dev.stockreading.market.event.EventFactory;
import tools.jackson.databind.ObjectMapper;

@Component
class DailyPublisher {

    private static final Logger log = LoggerFactory.getLogger(DailyPublisher.class);

    // 일봉이 없는 ETF를 null과 이유로 담는 v2 계약.
    private static final int SCHEMA_VERSION = 2;

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    DailyPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 요청의 correlationId를 이어받아 응답을 발행한다. 메시지 키는 jobId다(데일리 판에는 종목 하나가 없다). */
    void publish(DailyPayload payload, String requestEventId, String correlationId) {
        Envelope<DailyPayload> event = EventFactory.create(
                clock, Topics.MARKET_DAILY_RESPONDED, SCHEMA_VERSION, OffsetDateTime.now(clock), correlationId, payload);
        kafka.send(Topics.MARKET_DAILY_RESPONDED, payload.jobId(), mapper.writeValueAsString(event));
        // 종목코드·티커·원문은 보유 종목과 자료 내용이 드러나므로 식별자와 건수만 남긴다.
        long tickers = payload.etfs().stream().mapToLong(etf -> etf.tickers().size()).sum();
        long noDaily = payload.etfs().stream().filter(etf -> etf.missingReason() != null).count();
        long ok = payload.background().stream().filter(b -> "ok".equals(b.status())).count();
        long tooLarge = payload.background().stream().filter(b -> "too-large".equals(b.status())).count();
        log.info("daily responded jobId={} seq={} requestEventId={} etfs={} noDaily={} truncated={} tickers={} closeRates={} "
                        + "background ok={} tooLarge={} missing={} corr={}",
                payload.jobId(), payload.seq(), requestEventId, payload.etfs().size(), noDaily, payload.truncated(), tickers,
                payload.closeRates().size(), ok, tooLarge, payload.background().size() - ok - tooLarge,
                correlationId);
    }
}

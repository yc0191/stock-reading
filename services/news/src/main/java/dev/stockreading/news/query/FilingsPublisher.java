package dev.stockreading.news.query;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.Envelope;
import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.event.FilingsPayload;
import tools.jackson.databind.ObjectMapper;

@Component
class FilingsPublisher {

    private static final Logger log = LoggerFactory.getLogger(FilingsPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    FilingsPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 요청의 jobId, seq, correlationId를 그대로 이어받아 응답을 발행한다. 메시지 키는 종목코드다. */
    void publish(String jobId, int seq, String symbol, List<FilingsPayload.Filing> filings, boolean truncated,
            String requestEventId, String correlationId) {
        Envelope<FilingsPayload> event = EventFactory.create(
                clock, Topics.NEWS_FILINGS_RESPONDED, OffsetDateTime.now(clock), correlationId,
                new FilingsPayload(jobId, seq, symbol, filings, truncated));
        kafka.send(Topics.NEWS_FILINGS_RESPONDED, symbol, mapper.writeValueAsString(event));
        // 종목코드·티커·접수번호는 보유 종목이 드러나므로 식별자와 건수만 남긴다.
        log.info("filings responded jobId={} seq={} requestEventId={} filings={} truncated={} corr={}",
                jobId, seq, requestEventId, filings.size(), truncated, correlationId);
    }
}

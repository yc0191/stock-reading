package dev.stockreading.news.query;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.ArticlesPayload;
import dev.stockreading.news.event.Envelope;
import dev.stockreading.news.event.EventFactory;
import tools.jackson.databind.ObjectMapper;

@Component
class ArticlesPublisher {

    private static final Logger log = LoggerFactory.getLogger(ArticlesPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    ArticlesPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 요청의 jobId, seq, correlationId를 그대로 이어받아 응답을 발행한다. 메시지 키는 종목코드다. */
    void publish(String jobId, int seq, String symbol, List<ArticlesPayload.Article> articles,
            boolean truncated, String correlationId) {
        Envelope<ArticlesPayload> event = EventFactory.create(
                clock, "news.articles.responded", OffsetDateTime.now(clock), correlationId,
                new ArticlesPayload(jobId, seq, symbol, articles, truncated));
        kafka.send(Topics.NEWS_ARTICLES_RESPONDED, symbol, mapper.writeValueAsString(event));
        // 제목과 URL은 기사 내용이라 로그에 남기지 않고 식별자와 건수만 남긴다.
        log.info("{} articles responded jobId={} seq={} articles={} truncated={} corr={}",
                symbol, jobId, seq, articles.size(), truncated, correlationId);
    }
}

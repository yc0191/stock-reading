package dev.stockreading.news.article;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import tools.jackson.databind.ObjectMapper;

@Component
class NewsListener {

    private static final Logger log = LoggerFactory.getLogger(NewsListener.class);

    private final NewsArticleRepository repository;
    private final ObjectMapper mapper;

    NewsListener(NewsArticleRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.NEWS_COLLECTED, groupId = "news")
    void onNews(String message) {
        NewsEnvelope event = mapper.readValue(message, NewsEnvelope.class);
        NewsEnvelope.NewsPayload payload = event.payload();

        NewsArticle article = new NewsArticle(
                UUID.fromString(event.eventId()),
                payload.title(),
                payload.summary(),
                payload.url(),
                payload.publisher(),
                OffsetDateTime.parse(payload.publishedAt()),
                payload.symbols(),
                // matchedQueries는 선택 필드라 없으면 빈 목록으로 본다.
                payload.matchedQueries() == null ? List.of() : payload.matchedQueries());

        // 제목과 URL은 기사 내용이라 로그에 남기지 않고 식별자만 남긴다.
        boolean inserted = repository.save(article);
        log.info("news {} eventId={} corr={}",
                inserted ? "stored" : "merged into existing article", event.eventId(), event.correlationId());
    }
}

package dev.stockreading.news.query;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.ArticlesPayload;
import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.holdings.ReceivedHoldings;
import tools.jackson.databind.ObjectMapper;

@Component
class ArticlesRequestListener {

    /** 요청이 몇 건을 달라고 하든 이 건수까지만 읽는다. */
    static final int MAX_ARTICLES_CAP = 1000;

    /**
     * 응답 기사 목록의 직렬화 크기 예산(바이트). 한글 기사는 건당 최대 약 7.5KB라 건수만으로는 Kafka 메시지
     * 한도(기본 1MB)를 넘을 수 있고, 넘으면 발행이 거부되어 브리핑이 응답을 영영 받지 못한다. 봉투와 여유를
     * 위해 한도보다 작게 잡는다.
     */
    static final int MAX_ARTICLES_BYTES = 800_000;

    private static final Logger log = LoggerFactory.getLogger(ArticlesRequestListener.class);

    private final ArticleQueryRepository repository;
    private final ArticlesPublisher publisher;
    private final ObjectMapper mapper;
    private final ReceivedHoldings holdings;

    ArticlesRequestListener(ArticleQueryRepository repository, ArticlesPublisher publisher, ObjectMapper mapper,
            ReceivedHoldings holdings) {
        this.holdings = holdings;
        this.repository = repository;
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.NEWS_ARTICLES_REQUESTED, groupId = "news")
    void onRequest(String message) {
        ArticlesRequestEnvelope event = mapper.readValue(message, ArticlesRequestEnvelope.class);
        ArticlesRequestEnvelope.RequestPayload request = event.payload();
        OffsetDateTime since = OffsetDateTime.parse(request.since());
        OffsetDateTime until = OffsetDateTime.parse(request.until());
        if (request.maxArticles() < 1 || since.isAfter(until)) {
            // 잘못된 요청은 응답하지 않는다. 브리핑이 제한 시간 뒤 다시 보내거나 실패로 기록한다.
            throw new IllegalArgumentException("invalid articles request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }

        int limit = Math.min(request.maxArticles(), MAX_ARTICLES_CAP);
        // 한 건 더 읽어 건수 때문에 잘렸는지 판단한다.
        // ETF 로 꺼내면 그 ETF 자체 기사가 먼저, 구성 종목 기사가 그다음이다(태그는 기사 주제 그대로 두고 꺼낼 때 넓힌다).
        // 재검색 작업 id 가 있으면 그 작업이 찾은 기사만 읽고 종목 조건은 보지 않는다(symbol 은 응답 짝짓기에만 쓴다).
        List<ArticleQueryRepository.StoredArticle> rows = request.researchJobId() == null
                ? repository.findByRange(request.symbol(), holdings.membersOf(request.symbol()), since, until, limit + 1)
                : repository.findByResearchJob(UUID.fromString(request.researchJobId()), since, until, limit + 1);
        boolean truncated = rows.size() > limit;

        // 최신 기사부터 담고, 바이트 예산을 넘기는 기사부터는 버린다.
        List<ArticlesPayload.Article> articles = new ArrayList<>();
        int usedBytes = 0;
        for (ArticleQueryRepository.StoredArticle row : rows.subList(0, Math.min(rows.size(), limit))) {
            ArticlesPayload.Article article = new ArticlesPayload.Article(
                    row.articleKey(), row.title(), row.summary(),
                    EventFactory.formatKst(row.publishedAt()), row.publisher(), row.url());
            int size = mapper.writeValueAsBytes(article).length + 1;
            if (usedBytes + size > MAX_ARTICLES_BYTES) {
                truncated = true;
                break;
            }
            usedBytes += size;
            articles.add(article);
        }

        publisher.publish(request.jobId(), request.seq(), request.symbol(), articles, truncated, event.correlationId());
    }
}

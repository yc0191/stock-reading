package dev.stockreading.news.query;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.stereotype.Component;

import dev.stockreading.news.event.ArticlesPayload;
import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.etfmap.EtfStockMembers;
import tools.jackson.databind.ObjectMapper;

@Component
class ArticlesQuery {

    /** 요청이 몇 건을 달라고 하든 이 건수까지만 읽는다. */
    static final int MAX_ARTICLES_CAP = 1000;

    /**
     * 응답 기사 목록의 직렬화 크기 예산(바이트). 한글 기사는 건당 최대 약 7.5KB라 건수만으로는 응답 본문이
     * 크게 불어날 수 있다. Kafka 응답 때 메시지 한도(기본 1MB)에 맞춰 정한 값을 그대로 두어, 같은 요청에 예전과
     * 같은 내용을 준다.
     */
    static final int MAX_ARTICLES_BYTES = 800_000;

    private final ArticleQueryRepository repository;
    private final ObjectMapper mapper;
    private final EtfStockMembers members;

    ArticlesQuery(ArticleQueryRepository repository, ObjectMapper mapper, EtfStockMembers members) {
        this.members = members;
        this.repository = repository;
        this.mapper = mapper;
    }

    /** 내부 조회 API 가 부른다. 같은 요청에는 같은 내용을 돌려준다. */
    ArticlesPayload answer(ArticlesRequest request) {
        OffsetDateTime since = OffsetDateTime.parse(request.since());
        OffsetDateTime until = OffsetDateTime.parse(request.until());
        if (request.maxArticles() < 1 || since.isAfter(until)) {
            // 잘못된 요청은 답하지 않는다. 내부 조회 API 는 먼저 400 으로 거른다.
            throw new IllegalArgumentException("invalid articles request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }

        int limit = Math.min(request.maxArticles(), MAX_ARTICLES_CAP);
        // 한 건 더 읽어 건수 때문에 잘렸는지 판단한다.
        // ETF 로 꺼내면 그 ETF 자체 기사가 먼저, 구성 종목 기사가 그다음이다(태그는 기사 주제 그대로 두고 꺼낼 때 넓힌다).
        // 재검색 작업 id 가 있으면 그 작업이 찾은 기사만 읽고 종목 조건은 보지 않는다(symbol 은 계약상 필수 칸일 뿐이다).
        List<ArticleQueryRepository.StoredArticle> rows = request.researchJobId() == null
                ? repository.findByRange(request.symbol(), members.membersOf(request.symbol()), since, until, limit + 1)
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

        return new ArticlesPayload(request.jobId(), request.seq(), request.symbol(), articles, truncated);
    }
}

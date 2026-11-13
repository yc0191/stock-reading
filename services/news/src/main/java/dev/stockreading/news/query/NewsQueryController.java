package dev.stockreading.news.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import dev.stockreading.news.api.RequestChecks;
import dev.stockreading.news.event.ArticlesPayload;
import dev.stockreading.news.event.FilingsPayload;

/**
 * 기사(news.articles)와 공시(news.filings) 요청·응답의 내부 조회 API. 본문은 Kafka 요청 payload 와 같고, 답은 Kafka 응답
 * payload 와 같은 내용이다(같은 answer 를 쓴다). 추적 ID 는 요청 헤더 X-Correlation-Id 로 받는다.
 */
@RestController
@RequestMapping("/internal/news")
class NewsQueryController {

    private static final Logger log = LoggerFactory.getLogger(NewsQueryController.class);

    private final ArticlesRequestListener articles;
    private final FilingsRequestListener filings;

    NewsQueryController(ArticlesRequestListener articles, FilingsRequestListener filings) {
        this.articles = articles;
        this.filings = filings;
    }

    @PostMapping("/articles")
    ArticlesPayload articles(@RequestBody ArticlesRequestEnvelope.RequestPayload request) {
        check(request);
        ArticlesPayload payload = articles.answer(request);
        // 제목·URL·종목코드는 기사 내용과 보유 종목이 드러나므로 식별자와 건수만 남긴다.
        log.info("articles answered jobId={} seq={} articles={} truncated={}", payload.jobId(), payload.seq(),
                payload.articles().size(), payload.truncated());
        return payload;
    }

    @PostMapping("/filings")
    FilingsPayload filings(@RequestBody FilingsRequestEnvelope.RequestPayload request) {
        check(request);
        FilingsPayload payload = filings.answer(request);
        log.info("filings answered jobId={} seq={} filings={} truncated={}", payload.jobId(), payload.seq(),
                payload.filings().size(), payload.truncated());
        return payload;
    }

    // answer 가 Kafka 요청에서 거르는 조건과 같다. 여기서 먼저 걸러 400 으로 답한다.
    private static void check(ArticlesRequestEnvelope.RequestPayload request) {
        RequestChecks checks = RequestChecks.of(request.jobId());
        checks.positive("seq", request.seq());
        checks.text("symbol", request.symbol());
        checks.notAfter("since", checks.time("since", request.since()), checks.time("until", request.until()));
        checks.positive("maxArticles", request.maxArticles());
        // 재검색 작업 id 는 재검색 뒤 다시 물을 때만 보내는 선택 칸이다.
        if (request.researchJobId() != null) {
            checks.uuid("researchJobId", request.researchJobId());
        }
    }

    private static void check(FilingsRequestEnvelope.RequestPayload request) {
        RequestChecks checks = RequestChecks.of(request.jobId());
        checks.positive("seq", request.seq());
        checks.text("symbol", request.symbol());
        checks.notEmpty("tickers", request.tickers());
        checks.notAfter("since", checks.time("since", request.since()), checks.time("until", request.until()));
        checks.positive("maxFilings", request.maxFilings());
    }
}

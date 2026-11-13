package dev.stockreading.market.history;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import dev.stockreading.market.api.RequestChecks;
import dev.stockreading.market.event.HistoryPayload;

/**
 * 가격 이력(market.history) 요청·응답의 내부 조회 API. 본문은 Kafka 요청 payload 와 같고, 답은 Kafka 응답 payload 와
 * 같은 내용이다(같은 answer 를 쓴다). 추적 ID 는 요청 헤더 X-Correlation-Id 로 받는다.
 */
@RestController
class HistoryQueryController {

    private static final Logger log = LoggerFactory.getLogger(HistoryQueryController.class);

    private final HistoryRequestListener history;

    HistoryQueryController(HistoryRequestListener history) {
        this.history = history;
    }

    @PostMapping("/internal/market/history")
    HistoryPayload history(@RequestBody HistoryRequestEnvelope.RequestPayload request) {
        check(request);
        HistoryPayload payload = history.answer(request);
        log.info("history answered jobId={} seq={} points={} truncated={}", payload.jobId(), payload.seq(),
                payload.points().size(), payload.truncated());
        return payload;
    }

    // answer 가 Kafka 요청에서 거르는 조건과 같다. 여기서 먼저 걸러 400 으로 답한다.
    private static void check(HistoryRequestEnvelope.RequestPayload request) {
        RequestChecks checks = RequestChecks.of(request.jobId());
        checks.positive("seq", request.seq());
        checks.text("symbol", request.symbol());
        checks.notAfter("since", checks.time("since", request.since()), checks.time("until", request.until()));
        checks.positive("maxPoints", request.maxPoints());
        // 솎는 간격은 선택 칸이다.
        if (request.intervalSeconds() != null) {
            checks.positive("intervalSeconds", request.intervalSeconds());
        }
    }
}

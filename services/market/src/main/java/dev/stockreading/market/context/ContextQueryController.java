package dev.stockreading.market.context;

import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import dev.stockreading.market.api.RequestChecks;
import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.DailyPayload;

/**
 * 시장 배경과 데일리 시세의 내부 조회 API. 본문과 답은 옛 market.context·market.daily 요청·응답 이벤트의
 * payload 와 같은 모양이다(contracts/events/). 추적 ID 는 요청 헤더 X-Correlation-Id 로 받는다.
 */
@RestController
@RequestMapping("/internal/market")
class ContextQueryController {

    private static final Logger log = LoggerFactory.getLogger(ContextQueryController.class);

    private final ContextQuery context;
    private final DailyQuery daily;

    ContextQueryController(ContextQuery context, DailyQuery daily) {
        this.context = context;
        this.daily = daily;
    }

    @PostMapping("/context")
    ContextPayload context(@RequestBody ContextRequest request) {
        check(request);
        ContextPayload payload = context.answer(request);
        // 종목코드·티커·원문은 보유 종목과 자료 내용이 드러나므로 식별자와 건수만 남긴다.
        log.info("context answered jobId={} seq={} tickers={} background={}", payload.jobId(), payload.seq(),
                payload.tickers().size(), payload.background().size());
        return payload;
    }

    @PostMapping("/daily")
    DailyPayload daily(@RequestBody DailyRequest request) {
        check(request);
        DailyPayload payload = daily.answer(request);
        log.info("daily answered jobId={} seq={} etfs={} truncated={}", payload.jobId(), payload.seq(),
                payload.etfs().size(), payload.truncated());
        return payload;
    }

    // answer 가 거르는 조건과 같다. 여기서 먼저 걸러 400 으로 답한다.
    private static void check(ContextRequest request) {
        RequestChecks checks = RequestChecks.of(request.jobId());
        checks.positive("seq", request.seq());
        checks.text("symbol", request.symbol());
        OffsetDateTime asOf = checks.time("asOf", request.asOf());
        checks.notAfter("since", checks.time("since", request.since()), asOf);
        checks.positive("maxResponseBytes", request.maxResponseBytes());
        checks.positive("maxTickers", request.maxTickers());
    }

    private static void check(DailyRequest request) {
        RequestChecks checks = RequestChecks.of(request.jobId());
        checks.positive("seq", request.seq());
        OffsetDateTime asOf = checks.time("asOf", request.asOf());
        checks.notAfter("since", checks.time("since", request.since()), asOf);
        checks.positive("maxResponseBytes", request.maxResponseBytes());
        checks.positive("maxEtfs", request.maxEtfs());
        checks.positive("maxTickers", request.maxTickers());
        // 개장 흐름 시작 시각은 아침 판만 보내는 선택 칸이다.
        if (request.openFlowSince() != null) {
            checks.notAfter("openFlowSince", checks.time("openFlowSince", request.openFlowSince()), asOf);
        }
    }
}

package dev.stockreading.market.context;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 내부 조회 API 의 요청 본문. 옛 market.daily 요청 이벤트의 payload 와 같은 모양이다(contracts/events/).
 * 모르는 필드는 무시한다(호환 규칙, ADR-010).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
record DailyRequest(
        String jobId, int seq, String asOf, String since, int maxResponseBytes, int maxEtfs, int maxTickers,
        Boolean includeOperations, String openFlowSince, Boolean includeIntradayFallback) {

    boolean wantsOperations() {
        return Boolean.TRUE.equals(includeOperations);
    }

    boolean wantsIntradayFallback() {
        return Boolean.TRUE.equals(includeIntradayFallback);
    }
}

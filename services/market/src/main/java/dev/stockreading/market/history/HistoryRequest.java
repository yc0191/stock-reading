package dev.stockreading.market.history;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 내부 조회 API 의 요청 본문. 옛 market.history 요청 이벤트의 payload 와 같은 모양이다(contracts/events/).
 * 모르는 필드는 무시한다(호환 규칙, ADR-010).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
record HistoryRequest(
        String jobId, int seq, String symbol, String since, String until, int maxPoints, Integer intervalSeconds) {
}

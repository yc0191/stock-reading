package dev.stockreading.news.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 내부 조회 API 의 요청 본문. 옛 news.filings 요청 이벤트의 payload 와 같은 모양이다(contracts/events/).
 * 모르는 필드는 무시한다(호환 규칙, ADR-010).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
record FilingsRequest(
        String jobId, int seq, String symbol, List<String> tickers, String since, String until, int maxFilings) {
}

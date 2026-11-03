package dev.stockreading.news.event;

import java.util.List;

/** news.filings.responded의 payload. jobId와 seq는 요청 값을 그대로 되돌려준다. 공시 본문은 담지 않는다. */
public record FilingsPayload(String jobId, int seq, String symbol, List<Filing> filings, boolean truncated) {

    public record Filing(
            String accessionNumber,
            List<String> tickers,
            String form,
            List<String> items,
            String acceptedAt,
            String url) {
    }
}

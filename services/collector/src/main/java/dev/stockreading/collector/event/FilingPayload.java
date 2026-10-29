package dev.stockreading.collector.event;

import java.util.List;

/** filing.collected 의 payload. 공시 본문은 담지 않는다. */
public record FilingPayload(
        List<String> tickers,
        String cik,
        String accessionNumber,
        String form,
        List<String> items,
        String acceptedAt,
        String filingDate,
        String url) {
}

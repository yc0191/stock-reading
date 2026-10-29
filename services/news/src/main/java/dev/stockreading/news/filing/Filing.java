package dev.stockreading.news.filing;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/** 저장할 공시 한 건. 본문은 저장하지 않으므로 메타데이터만 가진다. */
record Filing(
        UUID eventId,
        String accessionNumber,
        String cik,
        String form,
        List<String> items,
        OffsetDateTime acceptedAt,
        LocalDate filingDate,
        String url,
        List<String> tickers) {
}

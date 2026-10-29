package dev.stockreading.news.filing;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

/** 조회 결과로 돌려주는 공시 한 건. tickers는 이 공시에 연결된 보유 티커 전체다. */
public record StoredFiling(
        String accessionNumber,
        String cik,
        String form,
        List<String> items,
        OffsetDateTime acceptedAt,
        LocalDate filingDate,
        String url,
        List<String> tickers) {
}

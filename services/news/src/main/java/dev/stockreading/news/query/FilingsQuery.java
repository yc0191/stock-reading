package dev.stockreading.news.query;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;

import org.springframework.stereotype.Component;

import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.event.FilingsPayload;

/**
 * 요청한 미국 종목 티커들의 공시 메타데이터를 접수 시각 기간으로 답한다. 저장 상태를 바꾸지 않는 조회라 같은 요청을
 * 다시 받아도 같은 내용으로 다시 답할 뿐 부작용이 없다(브리핑은 연결 실패·시간 초과 때 같은 요청을 다시 부른다).
 */
@Component
class FilingsQuery {

    /** 요청이 몇 건을 달라고 하든 이 건수까지만 읽는다. 공시 한 건은 수백 바이트라 응답 본문이 크지 않다. */
    static final int MAX_FILINGS_CAP = 1000;

    private final FilingQueryRepository repository;

    FilingsQuery(FilingQueryRepository repository) {
        this.repository = repository;
    }

    /** 내부 조회 API 가 부른다. 같은 요청에는 같은 내용을 돌려준다. */
    FilingsPayload answer(FilingsRequest request) {
        OffsetDateTime since = OffsetDateTime.parse(request.since());
        OffsetDateTime until = OffsetDateTime.parse(request.until());
        if (request.maxFilings() < 1 || since.isAfter(until) || request.tickers() == null
                || request.tickers().isEmpty()) {
            // 잘못된 요청은 답하지 않는다. 내부 조회 API 는 먼저 400 으로 거른다.
            throw new IllegalArgumentException("invalid filings request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }

        // 수집기는 종류 구분을 "-" 로 맞춰 저장하므로 "." 로 요청해도 같은 종목을 찾게 한다.
        List<String> tickers = request.tickers().stream()
                .map(ticker -> ticker.trim().toUpperCase(Locale.ROOT).replace('.', '-'))
                .distinct()
                .toList();
        int limit = Math.min(request.maxFilings(), MAX_FILINGS_CAP);
        // 한 건 더 읽어 잘렸는지 판단한다. 최신순이라 잘리면 오래된 공시가 빠진다.
        List<FilingQueryRepository.StoredFiling> rows = repository.findByTickers(tickers, since, until, limit + 1);
        boolean truncated = rows.size() > limit;
        List<FilingsPayload.Filing> filings = rows.stream()
                .limit(limit)
                .map(row -> new FilingsPayload.Filing(row.accessionNumber(), row.tickers(), row.form(), row.items(),
                        EventFactory.formatKst(row.acceptedAt()), row.url()))
                .toList();

        return new FilingsPayload(request.jobId(), request.seq(), request.symbol(), filings, truncated);
    }
}

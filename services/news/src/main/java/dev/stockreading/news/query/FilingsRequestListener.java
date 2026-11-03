package dev.stockreading.news.query;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.event.FilingsPayload;
import tools.jackson.databind.ObjectMapper;

/**
 * 요청한 미국 종목 티커들의 공시 메타데이터를 접수 시각 기간으로 답한다. 저장 상태를 바꾸지 않는 조회라 같은 요청
 * (eventId)을 다시 받아도 같은 내용으로 다시 답할 뿐 부작용이 없고, 브리핑은 jobId·seq로 짝을 지어 중복 응답을 버린다.
 */
@Component
class FilingsRequestListener {

    /** 요청이 몇 건을 달라고 하든 이 건수까지만 읽는다. 공시 한 건은 수백 바이트라 Kafka 메시지 한도 안에 든다. */
    static final int MAX_FILINGS_CAP = 1000;

    private final FilingQueryRepository repository;
    private final FilingsPublisher publisher;
    private final ObjectMapper mapper;

    FilingsRequestListener(FilingQueryRepository repository, FilingsPublisher publisher, ObjectMapper mapper) {
        this.repository = repository;
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.NEWS_FILINGS_REQUESTED, groupId = "news")
    void onRequest(String message) {
        FilingsRequestEnvelope event = mapper.readValue(message, FilingsRequestEnvelope.class);
        FilingsRequestEnvelope.RequestPayload request = event.payload();
        OffsetDateTime since = OffsetDateTime.parse(request.since());
        OffsetDateTime until = OffsetDateTime.parse(request.until());
        if (request.maxFilings() < 1 || since.isAfter(until) || request.tickers() == null
                || request.tickers().isEmpty()) {
            // 잘못된 요청은 응답하지 않는다. 브리핑이 제한 시간 뒤 다시 보내거나 실패로 기록한다.
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

        publisher.publish(request.jobId(), request.seq(), request.symbol(), filings, truncated, event.eventId(),
                event.correlationId());
    }
}

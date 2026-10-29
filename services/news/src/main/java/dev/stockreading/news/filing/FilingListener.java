package dev.stockreading.news.filing;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import tools.jackson.databind.ObjectMapper;

/**
 * 공시를 저장한다. 처리에 실패하면 예외를 그대로 던져, 서비스 공용 오류 처리기가 재시도한 뒤
 * failed_event에 남기게 한다(뉴스와 같은 처리기, ErrorHandlerConfig).
 */
@Component
class FilingListener {

    static final String GROUP_ID = "news-filings";

    private static final Logger log = LoggerFactory.getLogger(FilingListener.class);

    private final FilingRepository repository;
    private final ObjectMapper mapper;

    FilingListener(FilingRepository repository, ObjectMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    // 뉴스와 오프셋을 따로 관리하도록 별도 consumer group으로 구독한다.
    @KafkaListener(topics = Topics.FILING_COLLECTED, groupId = GROUP_ID)
    void onFiling(String message) {
        FilingEnvelope event = mapper.readValue(message, FilingEnvelope.class);
        FilingEnvelope.FilingPayload payload = event.payload();

        Filing filing = new Filing(
                UUID.fromString(event.eventId()),
                payload.accessionNumber(),
                payload.cik(),
                payload.form(),
                payload.items(),
                OffsetDateTime.parse(payload.acceptedAt()),
                LocalDate.parse(payload.filingDate()),
                payload.url(),
                payload.tickers());

        // 티커와 CIK는 보유 종목이 드러나므로 로그에는 식별자만 남긴다.
        boolean inserted = repository.save(filing);
        log.info("filing {} eventId={} corr={}",
                inserted ? "stored" : "merged into existing filing", event.eventId(), event.correlationId());
    }
}

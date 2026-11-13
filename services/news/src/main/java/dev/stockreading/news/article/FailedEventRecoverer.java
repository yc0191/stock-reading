package dev.stockreading.news.article;

import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/** 재시도를 모두 소진한 레코드를 건너뛰기 전에 DB에 남겨, 나중에 내용을 눈으로 확인할 수 있게 한다. */
@Component
class FailedEventRecoverer implements ConsumerRecordRecoverer {

    static final int MAX_MESSAGE_LENGTH = 1000;

    private static final Logger log = LoggerFactory.getLogger(FailedEventRecoverer.class);

    private final FailedEventRepository repository;
    private final ArticleResultRepository results;
    private final ObjectMapper mapper;

    FailedEventRecoverer(FailedEventRepository repository, ArticleResultRepository results, ObjectMapper mapper) {
        this.repository = repository;
        this.results = results;
        this.mapper = mapper;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        Throwable cause = rootCause(exception);
        // 저장에 실패해도 예외를 삼키지 않는다. 던지면 오류 처리기가 레코드를 건너뛰지 않고 다시 시도하므로,
        // 보관하지 못한 채 유실되는 일이 없다.
        repository.save(new FailedEvent(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() == null ? null : record.key().toString(),
                record.value() == null ? null : record.value().toString(),
                cause.getClass().getName(),
                truncate(cause.getMessage()),
                ErrorHandlerConfig.DELIVERY_ATTEMPTS));
        if (Topics.NEWS_COLLECTED.equals(record.topic())) {
            recordArticleFailure(record);
        }
        log.warn("record skipped after retries topic={} partition={} offset={} cause={}",
                record.topic(), record.partition(), record.offset(), cause.getClass().getSimpleName());
    }

    // 저장 끝 알림이 이 기사를 '저장 실패'로 셀 수 있게 남긴다. 본문에서 eventId 를 읽지 못하면 남길 수 없으므로,
    // 그 기사는 대기 한도가 지나 '처리되지 않음'으로 알려진다.
    private void recordArticleFailure(ConsumerRecord<?, ?> record) {
        UUID eventId = eventIdOf(record.value());
        if (eventId == null) {
            log.warn("failed article has no readable eventId topic={} partition={} offset={}",
                    record.topic(), record.partition(), record.offset());
            return;
        }
        results.record(eventId, ArticleResult.FAILED, correlationIdOf(record.value()));
    }

    private UUID eventIdOf(Object value) {
        try {
            JsonNode eventId = mapper.readTree(value.toString()).get("eventId");
            return eventId == null ? null : UUID.fromString(eventId.asString());
        } catch (RuntimeException e) {
            return null;
        }
    }

    private String correlationIdOf(Object value) {
        JsonNode correlationId = mapper.readTree(value.toString()).get("correlationId");
        return correlationId == null || correlationId.isNull() ? null : correlationId.asString();
    }

    // 감싸진 예외(리스너 실행 예외 등)가 아니라 실제 원인을 남겨야 무엇이 문제였는지 알 수 있다.
    private static Throwable rootCause(Throwable exception) {
        Throwable current = exception;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static String truncate(String message) {
        if (message == null || message.length() <= MAX_MESSAGE_LENGTH) {
            return message;
        }
        return message.substring(0, MAX_MESSAGE_LENGTH);
    }
}

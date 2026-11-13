package dev.stockreading.news.article;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.Envelope;
import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.event.StoredPayload;
import tools.jackson.databind.ObjectMapper;

/**
 * 수집 완료(collection.completed)를 받아, 그 수집이 보낸 기사 이벤트가 모두 처리되면 저장 끝(news.stored)을 한 건 낸다.
 * 대기 한도까지 처리되지 않은 기사가 있으면 그 eventId 를 담아 한도에서 낸다. 브리핑은 이 알림 뒤 기사를 한 번만 조회한다.
 *
 * <p>기다리는 동안 이 리스너의 스레드만 멈추고, 기사를 저장하는 리스너(news.collected)는 다른 스레드라 계속 돈다.
 * 수집 완료의 오프셋은 저장 끝을 보낸 뒤에 확정되므로, 기다리는 중에 멈췄다 다시 켜지면 같은 수집 완료를 다시 받아 처리한다.
 *
 * <p>기다리지 않고 지금 결과로 바로 내는 경우가 둘 있다. 수집이 끝난 지 {@code staleAfter} 이상 지난 완료(밀려 있다가 늦게 받음)와,
 * 이미 한 번 기다린 완료를 전송 실패 등으로 다시 처리할 때다. 그래서 한 번의 처리는 대기 한도 + 전송 시간을 넘지 않는다.
 */
@Component
class StoredNotifier {

    static final String EVENT_TYPE = "news.stored";

    /** 이미 기다린 수집 완료를 기억하는 수. 15분마다 몇 건이라 재시도가 이어지는 동안 밀려나지 않을 만큼이면 된다. */
    static final int WAITED_CAPACITY = 1000;

    private static final Logger log = LoggerFactory.getLogger(StoredNotifier.class);

    private final ArticleResultRepository results;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final StoredProperties properties;
    // 이미 기다린 수집 완료의 eventId. 다시 시작하면 비므로 재시작 뒤에는 한 번 더 기다릴 수 있다.
    private final Set<String> waited = Collections.newSetFromMap(Collections.synchronizedMap(
            new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > WAITED_CAPACITY;
                }
            }));

    StoredNotifier(ArticleResultRepository results, KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock,
            StoredProperties properties) {
        this.results = results;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
        this.properties = properties;
    }

    // 한 번에 하나만 가져온다. 여러 건이 한꺼번에 와서 각각 한도까지 기다리면 poll 간격 상한(기본 5분)을 넘어
    // 소비자 그룹에서 빠지고 같은 묶음을 처음부터 다시 받는다.
    @KafkaListener(topics = Topics.COLLECTION_COMPLETED, groupId = "news", properties = "max.poll.records=1")
    void onCompleted(String message) {
        CollectionCompletedEnvelope event = mapper.readValue(message, CollectionCompletedEnvelope.class);
        CollectionCompletedEnvelope.CompletedPayload payload = event.payload();
        if (payload.articleEventIds() == null) {
            // 기사 목록이 없으면 무엇을 기다릴지 알 수 없다. 브리핑은 자기 기다림 한도로 받는다.
            log.info("collection.completed without articleEventIds; no news.stored eventId={} corr={}",
                    event.eventId(), event.correlationId());
            return;
        }
        Set<UUID> articleIds = new LinkedHashSet<>();
        payload.articleEventIds().forEach(id -> articleIds.add(UUID.fromString(id)));

        Map<UUID, ArticleResult> processed = awaitResults(articleIds, waitLimitFor(event));
        publish(event, articleIds, processed);
    }

    private Duration waitLimitFor(CollectionCompletedEnvelope event) {
        if (!waited.add(event.eventId())) {
            log.info("collection.completed handled again; no wait eventId={} corr={}",
                    event.eventId(), event.correlationId());
            return Duration.ZERO;
        }
        Instant finishedAt = finishedAt(event.payload().finishedAt());
        if (finishedAt != null && !clock.instant().isBefore(finishedAt.plus(properties.staleAfter()))) {
            log.info("collection.completed is stale; no wait eventId={} corr={} finishedAt={}",
                    event.eventId(), event.correlationId(), event.payload().finishedAt());
            return Duration.ZERO;
        }
        return properties.waitLimit();
    }

    // 읽지 못하는 시각이면 오래됐는지 알 수 없으므로 평소처럼 기다린다.
    private static Instant finishedAt(String value) {
        if (value == null) {
            return null;
        }
        try {
            return OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private Map<UUID, ArticleResult> awaitResults(Set<UUID> articleIds, Duration waitLimit) {
        Instant deadline = clock.instant().plus(waitLimit);
        Map<UUID, ArticleResult> processed = new HashMap<>();
        Set<UUID> waiting = new LinkedHashSet<>(articleIds);
        while (true) {
            processed.putAll(results.resultsOf(waiting));
            waiting.removeAll(processed.keySet());
            if (waiting.isEmpty() || !clock.instant().isBefore(deadline)) {
                return processed;
            }
            try {
                Thread.sleep(properties.pollInterval());
            } catch (InterruptedException e) {
                // 종료 중이면 지금까지의 결과로 알린다. 다시 던지면 오류 처리기가 재시도를 바로 소진하고 이 알림을 버린다.
                Thread.currentThread().interrupt();
                return processed;
            }
        }
    }

    private void publish(CollectionCompletedEnvelope completed, Set<UUID> articleIds,
            Map<UUID, ArticleResult> processed) {
        Map<ArticleResult, Integer> counts = new EnumMap<>(ArticleResult.class);
        processed.values().forEach(result -> counts.merge(result, 1, Integer::sum));
        List<String> unprocessed = articleIds.stream()
                .filter(id -> !processed.containsKey(id))
                .map(UUID::toString)
                .sorted()
                .toList();
        CollectionCompletedEnvelope.CompletedPayload source = completed.payload();
        StoredPayload payload = new StoredPayload(
                completed.eventId(),
                source.jobType(),
                source.scheduledFor(),
                source.startedAt(),
                source.incompleteSymbols() == null ? List.of() : source.incompleteSymbols(),
                source.researchJobId(),
                articleIds.size(),
                counts.getOrDefault(ArticleResult.STORED, 0),
                counts.getOrDefault(ArticleResult.MERGED, 0),
                counts.getOrDefault(ArticleResult.FAILED, 0),
                unprocessed);
        Envelope<StoredPayload> event = EventFactory.create(
                clock, storedEventId(completed.eventId()), EVENT_TYPE, OffsetDateTime.now(clock),
                completed.correlationId(), payload);
        // 전송이 확정될 때까지 기다린다. 실패하면 예외로 오류 처리기가 다시 시도해, 수집 완료를 확정한 채 알림을 잃지 않는다.
        kafka.send(Topics.NEWS_STORED, source.jobType(), mapper.writeValueAsString(event)).join();
        log.info("sent news.stored jobType={} jobId={} corr={} articles={} stored={} merged={} failed={} unprocessed={}",
                source.jobType(), source.researchJobId(), completed.correlationId(), payload.articleCount(),
                payload.storedCount(), payload.mergedCount(), payload.failedCount(), unprocessed.size());
    }

    // 같은 수집 완료를 다시 받으면 다시 낸다. eventId 가 같아야 브리핑이 eventId 로 한 번만 처리한다.
    static UUID storedEventId(String collectionEventId) {
        return UUID.nameUUIDFromBytes((EVENT_TYPE + ":" + collectionEventId).getBytes(StandardCharsets.UTF_8));
    }
}

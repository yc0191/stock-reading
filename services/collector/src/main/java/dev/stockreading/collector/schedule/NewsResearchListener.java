package dev.stockreading.collector.schedule;

import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ResearchRequestEnvelope;
import dev.stockreading.collector.event.ResearchRequestEnvelope.ResearchRequestPayload;
import tools.jackson.databind.ObjectMapper;

/**
 * 브리핑의 뉴스 재검색 요청(news.research.requested)을 받아, 요청한 검색어로 기존 뉴스 수집을 한 번 돌린다.
 * 기사와 완료 알림 발행은 NewsPublisher 가 정기 수집과 같은 방식으로 한다.
 */
@Component
class NewsResearchListener {

    private static final Logger log = LoggerFactory.getLogger(NewsResearchListener.class);
    private static final int MAX_REMEMBERED_REQUESTS = 1_000;
    private static final Pattern SYMBOL = Pattern.compile("^([0-9A-Z]{6}|\\*)$");

    private final NewsPublisher publisher;
    private final NewsResearchProperties properties;
    private final ObjectMapper mapper;

    // 메모리 기반 중복 방지. 재시작하면 초기화된다.
    private final Set<String> processedRequests = ConcurrentHashMap.newKeySet();

    NewsResearchListener(NewsPublisher publisher, NewsResearchProperties properties, ObjectMapper mapper) {
        this.publisher = publisher;
        this.properties = properties;
        this.mapper = mapper;
    }

    // schedule.tick 리스너들과 그룹을 나눠 둔다. 같은 그룹이면 토픽 배정이 섞여 어느 한쪽이 메시지를 놓칠 수 있다.
    // 검색어와 종목은 보유 종목을 드러낼 수 있어 로그에 남기지 않고 요청 식별자와 개수만 남긴다.
    @KafkaListener(topics = Topics.NEWS_RESEARCH_REQUESTED, groupId = "collector-news-research")
    void onRequest(String message) {
        ResearchRequestEnvelope request = mapper.readValue(message, ResearchRequestEnvelope.class);
        String eventId = request.eventId();
        ResearchRequestPayload payload = request.payload();
        if (!isValid(request)) {
            // 완료 알림의 필수 값(작업 id, 종목, 요청 시각)을 채울 수 없으므로 발행하지 않는다.
            log.warn("invalid research request ignored eventId={} corr={}", eventId, request.correlationId());
            return;
        }
        if (!processedRequests.add(eventId)) {
            log.info("duplicate research request ignored eventId={} jobId={}", eventId, payload.jobId());
            return;
        }
        if (processedRequests.size() > MAX_REMEMBERED_REQUESTS) {
            processedRequests.clear();
            processedRequests.add(eventId);
        }

        List<String> requested = usableQueries(payload.queries());
        List<String> used = requested.size() > properties.maxQueries()
                ? requested.subList(0, properties.maxQueries())
                : requested;
        if (used.size() < requested.size()) {
            log.warn("research request over the cap eventId={} jobId={} cap={} dropped={}",
                    eventId, payload.jobId(), properties.maxQueries(), requested.size() - used.size());
        }
        log.info("research request accepted eventId={} jobId={} seq={} jobKind={} queryCount={}",
                eventId, payload.jobId(), payload.seq(), payload.jobKind(), used.size());
        publisher.publishResearch(request.correlationId(), request.occurredAt(), payload.jobId(), payload.symbol(),
                List.copyOf(used));
    }

    private static boolean isValid(ResearchRequestEnvelope request) {
        ResearchRequestPayload payload = request.payload();
        if (request.eventId() == null || request.correlationId() == null || request.correlationId().isBlank()
                || payload == null || payload.symbol() == null || !SYMBOL.matcher(payload.symbol()).matches()) {
            return false;
        }
        try {
            UUID.fromString(payload.jobId());
            OffsetDateTime.parse(request.occurredAt());
            return true;
        } catch (IllegalArgumentException | NullPointerException | DateTimeParseException e) {
            return false;
        }
    }

    // 앞뒤 공백을 지우고, 빈 검색어와 같은 검색어는 한 번만 남긴다. 순서는 요청 순서를 지킨다.
    private static List<String> usableQueries(List<String> requested) {
        if (requested == null) {
            return List.of();
        }
        Set<String> unique = new LinkedHashSet<>();
        for (String q : requested) {
            if (q != null && !q.isBlank()) {
                unique.add(q.trim());
            }
        }
        return new ArrayList<>(unique);
    }
}

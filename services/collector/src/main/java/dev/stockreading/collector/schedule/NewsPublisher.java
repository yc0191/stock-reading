package dev.stockreading.collector.schedule;

import java.net.URI;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.CollectionCompletedPayload;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.event.NewsPayload;
import dev.stockreading.collector.news.NewsCollection;
import dev.stockreading.collector.news.NewsCollector;
import dev.stockreading.collector.news.NewsItem;
import tools.jackson.databind.ObjectMapper;

/**
 * 수집한 기사를 기사마다 news.collected 이벤트 한 건으로 발행한다. 메시지 키는 기사 url 이다.
 * 기사 전송이 모두 확정되면(성공·실패 포함) collection.completed 를 한 건 발행한다. 메시지 키는 jobType 이다.
 */
@Component
class NewsPublisher {

    static final String EVENT_TYPE = "news.collected";
    static final String COMPLETED_EVENT_TYPE = "collection.completed";

    private static final Logger log = LoggerFactory.getLogger(NewsPublisher.class);
    private static final int MAX_REMEMBERED_KEYS = 10_000;
    private static final int MAX_TITLE = 500;
    private static final int MAX_SUMMARY = 2_000;
    private static final Pattern SYMBOL = Pattern.compile("^([0-9A-Z]{6}|\\*)$");

    private final NewsCollector collector;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    // 메모리 기반 재발행 억제이므로 재시작하면 초기화된다. url 문자열이 그대로 같을 때만 막으므로
    // www. 이나 utm_ 만 다른 url 은 다시 발행될 수 있다. 중복은 소비 쪽 DB 가 최종으로 막는다.
    // url 만 키로 쓰면 같은 기사가 다른 검색어·종목으로 다시 걸렸을 때 소비 쪽(종목·검색어를 합집합으로
    // 합침)이 새 연결을 받을 기회를 영영 잃는다. 그래서 url 과 종목·검색어 조합을 함께 기억한다.
    private final Set<SentKey> sentKeys = ConcurrentHashMap.newKeySet();

    NewsPublisher(NewsCollector collector, KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.collector = collector;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    void publishCollected(String correlationId, String scheduledFor) {
        NewsCollection collection;
        try {
            collection = collector.collectWithStatus();
        } catch (RuntimeException e) {
            // 검색어나 외부 오류 원문이 섞일 수 있어 예외 종류만 남긴다.
            // 수집 호출 전체가 실패하면 어느 종목도 믿을 수 없으므로 완료 알림도 보내지 않는다.
            log.warn("news collect failed corr={} errorType={}", correlationId, e.getClass().getSimpleName());
            return;
        }
        List<NewsItem> items = collection.items();
        // 전송 실패는 콜백 스레드에서도 기록되므로 동시 접근에 안전한 집합과 카운터를 쓴다.
        Set<String> incomplete = ConcurrentHashMap.newKeySet();
        incomplete.addAll(collection.incompleteSymbols());
        AtomicInteger confirmed = new AtomicInteger();
        List<CompletableFuture<Void>> settled = new ArrayList<>();

        int published = 0;
        int alreadyPublished = 0;
        int invalid = 0;
        int sendFailed = 0;
        for (NewsItem item : items) {
            NewsPayload payload = toPayload(item);
            if (payload == null) {
                invalid++;
                continue;
            }
            SentKey sentKey = SentKey.of(payload);
            if (sentKeys.contains(sentKey)) {
                alreadyPublished++;
                continue;
            }
            CompletableFuture<SendResult<String, String>> future;
            try {
                Envelope<NewsPayload> event = EventFactory.create(
                        clock, EVENT_TYPE, item.publishedAt(), correlationId, payload);
                future = kafka.send(Topics.NEWS_COLLECTED, payload.url(), mapper.writeValueAsString(event));
            } catch (RuntimeException e) {
                sendFailed++;
                markIncomplete(incomplete, item);
                log.warn("news publish failed corr={} errorType={}", correlationId, e.getClass().getSimpleName());
                continue;
            }
            // 전송이 끝난 뒤에만 기록해야, 비동기로 실패한 기사가 다음 tick 에서 다시 발행된다.
            // 콜백은 다른 스레드에서 실행되므로 동시 접근에 안전한 집합을 쓴다.
            // 완료 알림은 이 처리가 끝난 단계들을 기다려야 기록·실패 집계가 끝난 뒤에 나간다.
            settled.add(future.handle((result, error) -> {
                if (error == null) {
                    remember(sentKey);
                    confirmed.incrementAndGet();
                } else {
                    markIncomplete(incomplete, item);
                    log.warn("news send failed corr={} errorType={}", correlationId, error.getClass().getSimpleName());
                }
                return (Void) null;
            }));
            published++;
        }

        log.info("sent {} news.collected events corr={} collected={} alreadyPublished={} invalid={} sendFailed={}",
                published, correlationId, items.size(), alreadyPublished, invalid, sendFailed);
        if (!items.isEmpty() && invalid == items.size()) {
            // 설정 실수로 모든 기사가 조용히 버려지는 일을 막기 위해 눈에 띄게 남긴다.
            log.warn("all {} collected items were skipped as invalid corr={}; check the news query settings",
                    invalid, correlationId);
        }

        CompletableFuture.allOf(settled.toArray(new CompletableFuture[0])).whenComplete((ignored, error) ->
                publishCompleted(correlationId, scheduledFor, collection.startedAt(), confirmed.get(), incomplete));
    }

    private void publishCompleted(String correlationId, String scheduledFor, OffsetDateTime startedAt,
            int publishedCount, Set<String> incomplete) {
        try {
            OffsetDateTime finishedAt = OffsetDateTime.now(clock);
            CollectionCompletedPayload payload = new CollectionCompletedPayload(
                    TickPublisher.JOB_COLLECT_NEWS,
                    scheduledFor,
                    EventFactory.format(startedAt),
                    EventFactory.format(finishedAt),
                    publishedCount,
                    new ArrayList<>(new TreeSet<>(incomplete)));
            Envelope<CollectionCompletedPayload> event = EventFactory.create(
                    clock, COMPLETED_EVENT_TYPE, finishedAt, correlationId, payload);
            kafka.send(Topics.COLLECTION_COMPLETED, TickPublisher.JOB_COLLECT_NEWS, mapper.writeValueAsString(event));
            log.info("sent collection.completed corr={} published={} incomplete={}",
                    correlationId, publishedCount, incomplete.size());
        } catch (RuntimeException e) {
            log.warn("collection.completed publish failed corr={} errorType={}", correlationId, e.getClass().getSimpleName());
        }
    }

    // 스키마를 어기는 종목코드는 담지 않는다. 종목이 없는 기사(발행에서 빠지는 기사)는 해당하지 않는다.
    private static void markIncomplete(Set<String> incomplete, NewsItem item) {
        if (item.symbols() == null) {
            return;
        }
        for (String symbol : item.symbols()) {
            if (symbol != null && SYMBOL.matcher(symbol).matches()) {
                incomplete.add(symbol);
            }
        }
    }

    private void remember(SentKey key) {
        if (sentKeys.size() >= MAX_REMEMBERED_KEYS) {
            sentKeys.clear();
        }
        sentKeys.add(key);
    }

    // 정렬한 목록으로 비교하므로 순서만 다른 조합은 같은 키이고, 구분자 충돌도 없다.
    private record SentKey(String url, List<String> symbols, List<String> matchedQueries) {

        static SentKey of(NewsPayload payload) {
            return new SentKey(
                    payload.url(),
                    payload.symbols().stream().sorted().toList(),
                    payload.matchedQueries().stream().sorted().toList());
        }
    }

    // 스키마를 어기는 기사는 null 을 돌려 발행에서 뺀다. 길이 초과만 잘라서 살린다.
    private static NewsPayload toPayload(NewsItem item) {
        if (isBlank(item.title()) || isBlank(item.url()) || isBlank(item.source()) || item.publishedAt() == null) {
            return null;
        }
        if (!isUri(item.url())) {
            return null;
        }
        if (item.symbols() == null || item.symbols().isEmpty()
                || !item.symbols().stream().allMatch(s -> s != null && SYMBOL.matcher(s).matches())) {
            return null;
        }
        Set<String> queries = item.matchedQueries() == null ? Set.of() : item.matchedQueries();
        return new NewsPayload(
                truncate(item.title(), MAX_TITLE),
                truncate(item.summary() == null ? "" : item.summary(), MAX_SUMMARY),
                item.url(),
                item.source(),
                EventFactory.format(item.publishedAt()),
                List.copyOf(item.symbols()),
                List.copyOf(queries));
    }

    // 스키마의 길이는 코드 포인트 수라서, 서로게이트 쌍이 중간에서 끊기지 않게 코드 포인트 단위로 자른다.
    static String truncate(String text, int maxCodePoints) {
        if (text.codePointCount(0, text.length()) <= maxCodePoints) {
            return text;
        }
        return text.substring(0, text.offsetByCodePoints(0, maxCodePoints));
    }

    private static boolean isBlank(String text) {
        return text == null || text.isBlank();
    }

    private static boolean isUri(String text) {
        try {
            return URI.create(text).isAbsolute();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}

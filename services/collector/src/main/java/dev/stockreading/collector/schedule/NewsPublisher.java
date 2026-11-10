package dev.stockreading.collector.schedule;

import java.net.URI;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
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
import dev.stockreading.collector.holdings.HoldingsPort;
import dev.stockreading.collector.news.NewsCollection;
import dev.stockreading.collector.news.NewsCollector;
import dev.stockreading.collector.news.NewsItem;
import dev.stockreading.collector.news.NewsQuery;
import tools.jackson.databind.ObjectMapper;

/**
 * 수집한 기사를 기사마다 news.collected 이벤트 한 건으로 발행한다. 메시지 키는 기사 url 이다.
 * 기사 전송이 모두 확정되면(성공·실패 포함) collection.completed 를 한 건 발행한다. 메시지 키는 jobType 이다.
 * 재검색(news.research.requested)도 같은 수집·발행 경로를 쓰고, 기사와 완료 알림에 재검색 작업 id 를 싣는다.
 */
@Component
class NewsPublisher {

    static final String EVENT_TYPE = "news.collected";
    static final String COMPLETED_EVENT_TYPE = "collection.completed";
    static final String JOB_RESEARCH_NEWS = "research-news";

    private static final Logger log = LoggerFactory.getLogger(NewsPublisher.class);
    private static final int MAX_REMEMBERED_KEYS = 10_000;
    private static final int MAX_TITLE = 500;
    private static final int MAX_SUMMARY = 2_000;
    // 기사 태그(news.collected)는 국내 코드, 미국 티커, "*"를 받는다. 완료 알림의 못 모은 종목은 국내 코드와 "*"만 받는다.
    private static final Pattern SYMBOL = Pattern.compile("^(\\*|[A-Z0-9]{1,6}([.-][A-Z0-9]{1,2})?)$");
    private static final Pattern COMPLETED_SYMBOL = Pattern.compile("^([0-9A-Z]{6}|\\*)$");

    private final NewsCollector collector;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final HoldingsPort holdings;

    // 메모리 기반 재발행 억제이므로 재시작하면 초기화된다. url 문자열이 그대로 같을 때만 막으므로
    // www. 이나 utm_ 만 다른 url 은 다시 발행될 수 있다. 중복은 소비 쪽 DB 가 최종으로 막는다.
    // url 만 키로 쓰면 같은 기사가 다른 검색어·종목으로 다시 걸렸을 때 소비 쪽(종목·검색어를 합집합으로
    // 합침)이 새 연결을 받을 기회를 영영 잃는다. 그래서 url 과 종목·검색어 조합을 함께 기억한다.
    private final Set<SentKey> sentKeys = ConcurrentHashMap.newKeySet();

    NewsPublisher(NewsCollector collector, KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock,
            HoldingsPort holdings) {
        this.collector = collector;
        this.holdings = holdings;
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
        publish(collection, correlationId, scheduledFor, TickPublisher.JOB_COLLECT_NEWS, null);
    }

    /**
     * 재검색 요청 하나를 처리한다. 검색어마다 기존 수집(같은 하루 호출 상한, 같은 거르기·중복 합치기)을 거치고,
     * 기사 태그는 요청의 종목 하나다. 완료 알림의 scheduledFor 는 요청 시각이다.
     */
    void publishResearch(String correlationId, String requestedAt, String researchJobId, String symbol,
            List<String> queries) {
        List<NewsQuery> newsQueries = queries.stream()
                .map(q -> new NewsQuery(q, new LinkedHashSet<>(List.of(symbol))))
                .toList();
        NewsCollection collection;
        if (newsQueries.isEmpty()) {
            // 쓸 검색어가 없으면 아무것도 찾지 못한 것이다. 브리핑이 결과 없음으로 판단하도록 못 모은 종목으로 알린다.
            collection = new NewsCollection(List.of(), Set.of(symbol), OffsetDateTime.now(clock));
        } else {
            try {
                collection = collector.collectWithStatus(newsQueries);
            } catch (RuntimeException e) {
                // 정기 수집과 달리 브리핑이 이 완료 알림을 기다리므로, 실패해도 그 종목을 못 모은 것으로 알린다.
                log.warn("news research collect failed corr={} jobId={} errorType={}",
                        correlationId, researchJobId, e.getClass().getSimpleName());
                collection = new NewsCollection(List.of(), Set.of(symbol), OffsetDateTime.now(clock));
            }
        }
        publish(collection, correlationId, requestedAt, JOB_RESEARCH_NEWS, researchJobId);
    }

    private void publish(NewsCollection collection, String correlationId, String scheduledFor, String jobType,
            String researchJobId) {
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
            NewsPayload payload = toPayload(item, researchJobId);
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

        log.info("sent {} news.collected events jobType={} corr={} collected={} alreadyPublished={} invalid={} sendFailed={}",
                published, jobType, correlationId, items.size(), alreadyPublished, invalid, sendFailed);
        if (!items.isEmpty() && invalid == items.size()) {
            // 설정 실수로 모든 기사가 조용히 버려지는 일을 막기 위해 눈에 띄게 남긴다.
            log.warn("all {} collected items were skipped as invalid corr={}; check the news query settings",
                    invalid, correlationId);
        }

        CompletableFuture<Void> allSettled = CompletableFuture.allOf(settled.toArray(new CompletableFuture[0]));
        Runnable sendCompletion = () -> publishCompleted(
                jobType, researchJobId, correlationId, scheduledFor, collection.startedAt(), confirmed.get(), incomplete);
        if (allSettled.isDone()) {
            sendCompletion.run();
        } else {
            // 전송 콜백은 Kafka 전송기의 네트워크 스레드에서 돈다. 거기서 처음 쓰는 토픽으로 보내면 토픽 정보를
            // 받아 올 그 스레드가 스스로 막혀, 최대 대기 시간 동안 시세를 포함한 이 서비스의 모든 전송이 멈춘다.
            allSettled.whenCompleteAsync((ignored, error) -> sendCompletion.run());
        }
    }

    private void publishCompleted(String jobType, String researchJobId, String correlationId, String scheduledFor,
            OffsetDateTime startedAt, int publishedCount, Set<String> incomplete) {
        try {
            OffsetDateTime finishedAt = OffsetDateTime.now(clock);
            CollectionCompletedPayload payload = new CollectionCompletedPayload(
                    jobType,
                    scheduledFor,
                    EventFactory.format(startedAt),
                    EventFactory.format(finishedAt),
                    publishedCount,
                    completedSymbols(incomplete),
                    researchJobId);
            Envelope<CollectionCompletedPayload> event = EventFactory.create(
                    clock, COMPLETED_EVENT_TYPE, finishedAt, correlationId, payload);
            kafka.send(Topics.COLLECTION_COMPLETED, jobType, mapper.writeValueAsString(event));
            log.info("sent collection.completed jobType={} jobId={} corr={} published={} incomplete={}",
                    jobType, researchJobId, correlationId, publishedCount, incomplete.size());
        } catch (RuntimeException e) {
            log.warn("collection.completed publish failed corr={} errorType={}", correlationId, e.getClass().getSimpleName());
        }
    }

    // 미국 티커로 건 검색이 불완전하면, 그 티커 기사가 함께 나가는 ETF의 수집이 불완전한 것이므로 ETF 코드로 바꿔 알린다.
    // 완료 알림 스키마가 국내 코드와 "*"만 받기 때문이다.
    private List<String> completedSymbols(Set<String> incomplete) {
        Set<String> result = new TreeSet<>();
        for (String symbol : incomplete) {
            if (COMPLETED_SYMBOL.matcher(symbol).matches()) {
                result.add(symbol);
            } else {
                result.addAll(holdings.etfsHolding(symbol));
            }
        }
        return new ArrayList<>(result);
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
    // 재검색 작업 id 도 키에 넣는다. 정기 수집으로 이미 보낸 기사라도 재검색이 찾았으면 news 가 그 작업의
    // 기사로 표시할 수 있게 다시 보내야 하기 때문이다.
    private record SentKey(String url, List<String> symbols, List<String> matchedQueries, String researchJobId) {

        static SentKey of(NewsPayload payload) {
            return new SentKey(
                    payload.url(),
                    payload.symbols().stream().sorted().toList(),
                    payload.matchedQueries().stream().sorted().toList(),
                    payload.researchJobId());
        }
    }

    // 스키마를 어기는 기사는 null 을 돌려 발행에서 뺀다. 길이 초과만 잘라서 살린다.
    private static NewsPayload toPayload(NewsItem item, String researchJobId) {
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
                List.copyOf(queries),
                researchJobId);
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

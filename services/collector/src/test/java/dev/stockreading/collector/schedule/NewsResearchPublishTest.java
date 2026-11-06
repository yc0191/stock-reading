package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.news.NewsCollection;
import dev.stockreading.collector.news.NewsCollector;
import dev.stockreading.collector.news.NewsItem;
import dev.stockreading.collector.news.NewsQuery;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 재검색 요청 하나가 기존 수집을 거쳐 기사(작업 id 포함)와 재검색 완료 알림 한 건으로 나가는지 확인한다. */
class NewsResearchPublishTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:16:40Z"), ZoneId.of("Asia/Seoul"));
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final String REQUESTED_AT = "2026-10-20T16:16:10+09:00";
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final OffsetDateTime STARTED_AT = OffsetDateTime.parse("2026-10-20T16:16:11+09:00");
    private static final List<String> QUERIES = List.of("테스트 재검색어 하나", "테스트 재검색어 둘");

    private NewsCollector collector;
    private KafkaTemplate<String, String> kafka;
    private NewsPublisher publisher;
    private ListAppender<ILoggingEvent> logs;
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        collector = mock(NewsCollector.class);
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher = new NewsPublisher(collector, kafka, mapper, CLOCK);
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(NewsPublisher.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(NewsPublisher.class)).detachAppender(logs);
    }

    @Test
    void eachRequestedQueryGoesThroughTheExistingCollectionTaggedWithTheRequestSymbol() {
        collect(Set.of());

        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", QUERIES);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<NewsQuery>> queries = ArgumentCaptor.forClass(List.class);
        verify(collector).collectWithStatus(queries.capture());
        assertThat(queries.getValue()).containsExactly(
                new NewsQuery("테스트 재검색어 하나", Set.of("TEST01")),
                new NewsQuery("테스트 재검색어 둘", Set.of("TEST01")));
    }

    @Test
    void articlesCarryTheResearchJobIdAndTheRequestSymbolAndMatchTheContract() throws IOException {
        collect(Set.of(), item("https://news.example.com/a", "TEST01"), item("https://news.example.com/b", "TEST01"));

        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", QUERIES);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), key.capture(), value.capture());
        assertThat(key.getAllValues()).containsExactly("https://news.example.com/a", "https://news.example.com/b");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate("news.collected.v1.schema.json", json)).isEmpty();
            JsonNode event = mapper.readTree(json);
            assertThat(event.get("correlationId").asString()).isEqualTo(CORRELATION_ID);
            assertThat(event.get("payload").get("researchJobId").asString()).isEqualTo(JOB_ID);
            assertThat(event.get("payload").get("symbols")).extracting(JsonNode::asString).containsExactly("TEST01");
        }
    }

    @Test
    void oneResearchCompletionKeyedByTheResearchJobTypeFollowsTheArticles() throws IOException {
        collect(Set.of(), item("https://news.example.com/a", "TEST01"));

        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", QUERIES);

        String json = completionJson();
        assertThat(ContractSchemas.validate("collection.completed.v1.schema.json", json)).isEmpty();
        JsonNode event = mapper.readTree(json);
        assertThat(event.get("correlationId").asString()).isEqualTo(CORRELATION_ID);
        JsonNode payload = event.get("payload");
        assertThat(payload.get("jobType").asString()).isEqualTo("research-news");
        assertThat(payload.get("researchJobId").asString()).isEqualTo(JOB_ID);
        assertThat(payload.get("scheduledFor").asString()).isEqualTo(REQUESTED_AT);
        assertThat(payload.get("startedAt").asString()).isEqualTo("2026-10-20T16:16:11+09:00");
        assertThat(payload.get("finishedAt").asString()).isEqualTo("2026-10-20T16:16:40+09:00");
        assertThat(payload.get("publishedCount").asInt()).isEqualTo(1);
        assertThat(payload.get("incompleteSymbols")).isEmpty();
    }

    @Test
    void dailyLimitOrQuerySearchFailurePutsTheRequestSymbolInIncompleteSymbols() throws IOException {
        // 하루 상한으로 건너뛴 검색어와 검색 호출이 실패한 검색어는 NewsCollector 가 그 종목을 못 모은 것으로 돌려준다.
        collect(Set.of("TEST01"));

        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", QUERIES);

        String json = completionJson();
        assertThat(ContractSchemas.validate("collection.completed.v1.schema.json", json)).isEmpty();
        JsonNode payload = mapper.readTree(json).get("payload");
        assertThat(payload.get("incompleteSymbols")).extracting(JsonNode::asString).containsExactly("TEST01");
        assertThat(payload.get("researchJobId").asString()).isEqualTo(JOB_ID);
    }

    @Test
    void wholeSearchFailureStillSendsTheCompletionWithTheSymbolIncomplete() throws IOException {
        // 정기 수집과 달리 브리핑이 이 완료를 기다리므로, 실패해도 완료를 보내야 브리핑이 시간 초과까지 기다리지 않는다.
        when(collector.collectWithStatus(anyList())).thenThrow(new IllegalStateException("search failed"));

        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "*", QUERIES);

        verify(kafka, never()).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());
        String json = completionJson();
        assertThat(ContractSchemas.validate("collection.completed.v1.schema.json", json)).isEmpty();
        JsonNode payload = mapper.readTree(json).get("payload");
        assertThat(payload.get("publishedCount").asInt()).isZero();
        assertThat(payload.get("incompleteSymbols")).extracting(JsonNode::asString).containsExactly("*");
    }

    @Test
    void noUsableQueryMeansNothingWasSearchedAndTheSymbolIsIncomplete() throws IOException {
        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", List.of());

        verify(collector, never()).collectWithStatus(anyList());
        JsonNode payload = mapper.readTree(completionJson()).get("payload");
        assertThat(payload.get("incompleteSymbols")).extracting(JsonNode::asString).containsExactly("TEST01");
    }

    @Test
    void articleAlreadySentByTheRegularCollectionIsSentAgainForTheResearchJob() {
        NewsItem regular = item("https://news.example.com/a", "TEST01");
        when(collector.collectWithStatus()).thenReturn(new NewsCollection(List.of(regular), Set.of(), STARTED_AT));
        publisher.publishCollected("collect-news-2026-10-20T16:15:00+09:00", "2026-10-20T16:15:00+09:00");
        collect(Set.of(), regular);

        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", QUERIES);

        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());
    }

    @Test
    void regularCompletionStillHasNoResearchJobId() throws IOException {
        when(collector.collectWithStatus()).thenReturn(new NewsCollection(List.of(), Set.of(), STARTED_AT));

        publisher.publishCollected("collect-news-2026-10-20T16:15:00+09:00", "2026-10-20T16:15:00+09:00");

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.COLLECTION_COMPLETED), eq("collect-news"), value.capture());
        assertThat(mapper.readTree(value.getValue()).get("payload").has("researchJobId")).isFalse();
    }

    @Test
    void logsCarryNeitherTheQueriesNorTheSymbol() {
        collect(Set.of("TEST01"), item("https://news.example.com/a", "TEST01"));

        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", QUERIES);
        when(collector.collectWithStatus(anyList())).thenThrow(new IllegalStateException("테스트 재검색어 하나"));
        publisher.publishResearch(CORRELATION_ID, REQUESTED_AT, JOB_ID, "TEST01", QUERIES);

        assertThat(logs.list).isNotEmpty();
        for (ILoggingEvent event : logs.list) {
            assertThat(event.getFormattedMessage()).doesNotContain("재검색어", "TEST01");
        }
    }

    private void collect(Set<String> incompleteSymbols, NewsItem... items) {
        when(collector.collectWithStatus(any())).thenReturn(new NewsCollection(List.of(items), incompleteSymbols, STARTED_AT));
    }

    private String completionJson() {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(1)).send(eq(Topics.COLLECTION_COMPLETED), eq("research-news"), value.capture());
        return value.getValue();
    }

    private static NewsItem item(String url, String symbol) {
        return new NewsItem("테스트 기사 제목", "테스트 기사 요약입니다.", url, null,
                OffsetDateTime.parse("2026-10-20T16:05:00+09:00"), "news.example.com",
                new LinkedHashSet<>(Set.of(symbol)), new LinkedHashSet<>(Set.of("테스트 재검색어 하나")));
    }
}

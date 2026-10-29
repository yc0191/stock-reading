package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.news.NewsCollection;
import dev.stockreading.collector.news.NewsCollector;
import dev.stockreading.collector.news.NewsItem;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class CollectionCompletedPublishTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:42Z"), ZoneId.of("Asia/Seoul"));
    private static final String CORRELATION_ID = "collect-news-2026-10-20T16:00:00+09:00";
    private static final String SCHEDULED_FOR = "2026-10-20T16:00:00+09:00";
    private static final OffsetDateTime STARTED_AT = OffsetDateTime.parse("2026-10-20T16:00:01+09:00");
    private static final String SCHEMA = "collection.completed.v1.schema.json";

    private NewsCollector collector;
    private KafkaTemplate<String, String> kafka;
    private NewsPublisher publisher;
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        collector = mock(NewsCollector.class);
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher = new NewsPublisher(collector, kafka, mapper, CLOCK);
    }

    @Test
    void publishesOneCompletionKeyedByJobTypeThatPassesTheContract() throws IOException {
        collect(Set.of(), item("https://news.example.com/a", "TEST01"), item("https://news.example.com/b", "TEST02"));

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        JsonNode event = mapper.readTree(completionJson());
        verify(kafka, times(1)).send(eq(Topics.COLLECTION_COMPLETED), eq("collect-news"), anyString());
        assertThat(ContractSchemas.validate(SCHEMA, completionJson())).isEmpty();
        assertThat(event.get("eventType").asString()).isEqualTo("collection.completed");
        assertThat(event.get("correlationId").asString()).isEqualTo(CORRELATION_ID);
        JsonNode payload = event.get("payload");
        assertThat(payload.get("jobType").asString()).isEqualTo("collect-news");
        assertThat(payload.get("scheduledFor").asString()).isEqualTo(SCHEDULED_FOR);
        assertThat(payload.get("startedAt").asString()).isEqualTo("2026-10-20T16:00:01+09:00");
        assertThat(payload.get("finishedAt").asString()).isEqualTo("2026-10-20T16:00:42+09:00");
        assertThat(event.get("occurredAt").asString()).isEqualTo(payload.get("finishedAt").asString());
        assertThat(payload.get("publishedCount").asInt()).isEqualTo(2);
        assertThat(payload.get("incompleteSymbols")).isEmpty();
    }

    @Test
    void queryFailureAndDailyLimitSkipSymbolsComeFromTheCollection() throws IOException {
        collect(new LinkedHashSet<>(List.of("TEST02", "TEST03")), item("https://news.example.com/a", "TEST01"));

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        assertThat(ContractSchemas.validate(SCHEMA, completionJson())).isEmpty();
        assertThat(incomplete()).containsExactly("TEST02", "TEST03");
    }

    @Test
    void marketWideSymbolIsKeptWhenItsQueryWasNotCollected() throws IOException {
        collect(Set.of("*"));

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        assertThat(incomplete()).containsExactly("*");
    }

    @Test
    void synchronousSendFailureMarksTheArticleSymbolsIncomplete() throws IOException {
        collect(Set.of(), item("https://news.example.com/a", "TEST01"), item("https://news.example.com/b", "TEST02"));
        doThrow(new IllegalStateException("broker down")).when(kafka)
                .send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        assertThat(incomplete()).containsExactly("TEST01");
        assertThat(mapper.readTree(completionJson()).get("payload").get("publishedCount").asInt()).isEqualTo(1);
    }

    @Test
    void asynchronousSendFailureMarksTheArticleSymbolsIncomplete() throws IOException {
        collect(Set.of(), item("https://news.example.com/a", "TEST01", "*"));
        when(kafka.send(eq(Topics.NEWS_COLLECTED), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("timeout")));

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        assertThat(incomplete()).containsExactly("*", "TEST01");
        assertThat(mapper.readTree(completionJson()).get("payload").get("publishedCount").asInt()).isZero();
    }

    @Test
    void completionWaitsUntilEverySendIsSettled() {
        collect(Set.of(), item("https://news.example.com/a", "TEST01"), item("https://news.example.com/b", "TEST02"));
        CompletableFuture<SendResult<String, String>> first = new CompletableFuture<>();
        CompletableFuture<SendResult<String, String>> second = new CompletableFuture<>();
        when(kafka.send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString())).thenReturn(first);
        when(kafka.send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/b"), anyString())).thenReturn(second);

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);
        verify(kafka, never()).send(eq(Topics.COLLECTION_COMPLETED), anyString(), anyString());

        first.complete(null);
        verify(kafka, never()).send(eq(Topics.COLLECTION_COMPLETED), anyString(), anyString());

        second.completeExceptionally(new IllegalStateException("timeout"));
        verify(kafka, times(1)).send(eq(Topics.COLLECTION_COMPLETED), eq("collect-news"), anyString());
    }

    @Test
    void completionIsPublishedEvenWhenThereIsNothingToSend() throws IOException {
        collect(Set.of());

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        assertThat(ContractSchemas.validate(SCHEMA, completionJson())).isEmpty();
        verify(kafka, never()).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());
    }

    @Test
    void articlesPublishedBeforeAreNotCountedOrMarkedIncomplete() throws IOException {
        collect(Set.of(), item("https://news.example.com/a", "TEST01"));
        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        publisher.publishCollected("collect-news-next", SCHEDULED_FOR);

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.COLLECTION_COMPLETED), eq("collect-news"), value.capture());
        JsonNode second = mapper.readTree(value.getAllValues().get(1));
        assertThat(second.get("payload").get("publishedCount").asInt()).isZero();
        assertThat(second.get("payload").get("incompleteSymbols")).isEmpty();
    }

    @Test
    void wholeCollectFailurePublishesNoCompletion() {
        when(collector.collectWithStatus()).thenThrow(new IllegalStateException("search failed"));

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void failureToPublishTheCompletionIsSwallowed() {
        collect(Set.of());
        doThrow(new IllegalStateException("broker down")).when(kafka)
                .send(eq(Topics.COLLECTION_COMPLETED), anyString(), anyString());

        publisher.publishCollected(CORRELATION_ID, SCHEDULED_FOR);

        verify(kafka, times(1)).send(eq(Topics.COLLECTION_COMPLETED), anyString(), anyString());
    }

    private void collect(Set<String> incompleteSymbols, NewsItem... items) {
        when(collector.collectWithStatus()).thenReturn(new NewsCollection(List.of(items), incompleteSymbols, STARTED_AT));
    }

    private String completionJson() {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.COLLECTION_COMPLETED), eq("collect-news"), value.capture());
        return value.getValue();
    }

    private List<String> incomplete() throws IOException {
        List<String> symbols = new java.util.ArrayList<>();
        mapper.readTree(completionJson()).get("payload").get("incompleteSymbols")
                .forEach(node -> symbols.add(node.asString()));
        return symbols;
    }

    private static NewsItem item(String url, String... symbols) {
        return new NewsItem("테스트 기사 제목", "테스트 기사 요약입니다.", url, null,
                OffsetDateTime.parse("2026-10-20T15:42:00+09:00"), "news.example.com",
                new LinkedHashSet<>(List.of(symbols)), new LinkedHashSet<>(Set.of("테스트검색어")));
    }
}

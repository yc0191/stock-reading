package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
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
import dev.stockreading.collector.news.NewsCollector;
import dev.stockreading.collector.news.NewsItem;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class NewsPublisherTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final String CORRELATION_ID = "collect-news-2026-10-20T16:00:00+09:00";
    private static final String SCHEMA = "news.collected.v1.schema.json";

    private NewsCollector collector;
    private KafkaTemplate<String, String> kafka;
    private NewsPublisher publisher;
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        collector = mock(NewsCollector.class);
        kafka = mock(KafkaTemplate.class);
        stubSendSucceeds();
        publisher = new NewsPublisher(collector, kafka, mapper, CLOCK);
    }

    @Test
    void publishesOneEventPerItemKeyedByUrlAndMatchingTheContract() throws IOException {
        when(collector.collect()).thenReturn(List.of(
                item("https://news.example.com/a", Set.of("TEST01")),
                item("https://news.example.com/b", Set.of("TEST02", "*"))));

        publisher.publishCollected(CORRELATION_ID);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), key.capture(), value.capture());
        assertThat(key.getAllValues()).containsExactly("https://news.example.com/a", "https://news.example.com/b");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        }
        JsonNode first = mapper.readTree(value.getAllValues().get(0));
        assertThat(first.get("eventType").asString()).isEqualTo("news.collected");
        assertThat(first.get("correlationId").asString()).isEqualTo(CORRELATION_ID);
        assertThat(first.get("occurredAt").asString()).isEqualTo(first.get("payload").get("publishedAt").asString());
        assertThat(first.get("payload").get("publisher").asString()).isEqualTo("news.example.com");
    }

    @Test
    void doesNotPublishTheSameUrlAgain() {
        when(collector.collect()).thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"))));

        publisher.publishCollected(CORRELATION_ID);
        publisher.publishCollected("collect-news-next");

        verify(kafka, times(1)).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());
    }

    @Test
    void sameUrlWithDifferentSymbolsIsPublishedAgain() {
        when(collector.collect())
                .thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"), Set.of("테스트검색어"))))
                .thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST02"), Set.of("테스트검색어"))));

        publisher.publishCollected(CORRELATION_ID);
        publisher.publishCollected("collect-news-next");

        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());
    }

    @Test
    void sameUrlWithDifferentMatchedQueriesIsPublishedAgain() {
        when(collector.collect())
                .thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"), Set.of("테스트검색어"))))
                .thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"), Set.of("다른검색어"))));

        publisher.publishCollected(CORRELATION_ID);
        publisher.publishCollected("collect-news-next");

        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());
    }

    @Test
    void sameUrlWithSameSymbolsAndQueriesInDifferentOrderIsNotPublishedAgain() {
        when(collector.collect())
                .thenReturn(List.of(item("https://news.example.com/a",
                        new LinkedHashSet<>(List.of("TEST01", "TEST02")), new LinkedHashSet<>(List.of("가", "나")))))
                .thenReturn(List.of(item("https://news.example.com/a",
                        new LinkedHashSet<>(List.of("TEST02", "TEST01")), new LinkedHashSet<>(List.of("나", "가")))));

        publisher.publishCollected(CORRELATION_ID);
        publisher.publishCollected("collect-news-next");

        verify(kafka, times(1)).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());
    }

    @Test
    void failedSendIsNotRememberedSoItIsRetriedOnTheNextRun() {
        when(collector.collect()).thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"))));
        doThrow(new IllegalStateException("broker down")).when(kafka).send(anyString(), anyString(), anyString());

        publisher.publishCollected(CORRELATION_ID);

        reset(kafka);
        stubSendSucceeds();
        publisher.publishCollected("collect-news-next");

        verify(kafka, times(1)).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());
    }

    @Test
    void asyncFailedSendIsNotRememberedSoItIsRetriedOnTheNextRun() {
        when(collector.collect()).thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"))));
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("timeout")))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishCollected(CORRELATION_ID);
        publisher.publishCollected("collect-news-next");
        publisher.publishCollected("collect-news-after");

        // 첫 전송은 실패해 기록되지 않았고, 두 번째는 성공해 기록되었으므로 세 번째 tick 에서는 보내지 않는다.
        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());
    }

    @Test
    void urlIsRecordedOnlyAfterTheSendFutureCompletes() {
        when(collector.collect()).thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"))));
        CompletableFuture<SendResult<String, String>> pending = new CompletableFuture<>();
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(pending);

        publisher.publishCollected(CORRELATION_ID);
        publisher.publishCollected("collect-news-next");
        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());

        pending.complete(null);
        publisher.publishCollected("collect-news-after");
        verify(kafka, times(2)).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());
    }

    @Test
    void oneFailedSendDoesNotStopTheOtherItems() {
        when(collector.collect()).thenReturn(List.of(
                item("https://news.example.com/a", Set.of("TEST01")),
                item("https://news.example.com/b", Set.of("TEST01"))));
        doThrow(new IllegalStateException("broker down")).when(kafka)
                .send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/a"), anyString());

        publisher.publishCollected(CORRELATION_ID);

        verify(kafka).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/b"), anyString());
    }

    @Test
    void collectFailureIsSwallowed() {
        when(collector.collect()).thenThrow(new IllegalStateException("search failed for 테스트검색어"));

        publisher.publishCollected(CORRELATION_ID);

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void itemsThatWouldBreakTheContractAreSkipped() {
        when(collector.collect()).thenReturn(List.of(
                item("https://news.example.com/no-symbols", Set.of()),
                item("https://news.example.com/bad-symbol", Set.of("TEST011")),
                item("not a url", Set.of("TEST01")),
                item("https://news.example.com/ok", Set.of("TEST01"))));

        publisher.publishCollected(CORRELATION_ID);

        verify(kafka, times(1)).send(eq(Topics.NEWS_COLLECTED), eq("https://news.example.com/ok"), anyString());
    }

    @Test
    void whenEveryItemIsSkippedNothingIsPublishedAndNothingIsRemembered() {
        when(collector.collect())
                .thenReturn(List.of(item("https://news.example.com/a", Set.of())))
                .thenReturn(List.of(item("https://news.example.com/a", Set.of("TEST01"))));

        publisher.publishCollected(CORRELATION_ID);
        verify(kafka, never()).send(anyString(), anyString(), anyString());

        publisher.publishCollected("collect-news-next");
        verify(kafka, times(1)).send(eq(Topics.NEWS_COLLECTED), anyString(), anyString());
    }

    @Test
    void overlongTitleAndSummaryAreTruncatedWithoutSplittingSurrogatePairs() throws IOException {
        String emoji = "😀";
        NewsItem long1 = new NewsItem(emoji.repeat(501), emoji.repeat(2001), "https://news.example.com/a",
                null, OffsetDateTime.parse("2026-10-20T15:42:00+09:00"), "news.example.com",
                Set.of("TEST01"), Set.of());
        when(collector.collect()).thenReturn(List.of(long1));

        publisher.publishCollected(CORRELATION_ID);

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.NEWS_COLLECTED), anyString(), value.capture());
        assertThat(ContractSchemas.validate(SCHEMA, value.getValue())).isEmpty();
        JsonNode payload = mapper.readTree(value.getValue()).get("payload");
        assertThat(payload.get("title").asString().codePointCount(0, payload.get("title").asString().length()))
                .isEqualTo(500);
        assertThat(payload.get("summary").asString().codePointCount(0, payload.get("summary").asString().length()))
                .isEqualTo(2000);
    }

    @Test
    void nullSummaryBecomesEmptyString() throws IOException {
        NewsItem item = new NewsItem("테스트 제목", null, "https://news.example.com/a", null,
                OffsetDateTime.parse("2026-10-20T15:42:00+09:00"), "news.example.com",
                Set.of("TEST01"), null);
        when(collector.collect()).thenReturn(List.of(item));

        publisher.publishCollected(CORRELATION_ID);

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.NEWS_COLLECTED), anyString(), value.capture());
        assertThat(ContractSchemas.validate(SCHEMA, value.getValue())).isEmpty();
    }

    private void stubSendSucceeds() {
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
    }

    private static NewsItem item(String url, Set<String> symbols) {
        return item(url, symbols, Set.of("테스트검색어"));
    }

    private static NewsItem item(String url, Set<String> symbols, Set<String> queries) {
        return new NewsItem("테스트 기사 제목", "테스트 기사 요약입니다.", url, null,
                OffsetDateTime.parse("2026-10-20T15:42:00+09:00"), "news.example.com",
                new LinkedHashSet<>(symbols), new LinkedHashSet<>(queries));
    }
}

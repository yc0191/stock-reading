package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.news.Topics;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class StoredNotifierTest {

    private static final String SCHEMA = "news.stored.v1.schema.json";
    private static final String COMPLETION_ID = "b3f1a6d2-84c7-4e0b-9a52-1d7c3e8f6a40";
    private static final UUID A = UUID.fromString("e0000001-0000-4000-8000-000000000001");
    private static final UUID B = UUID.fromString("e0000002-0000-4000-8000-000000000002");
    private static final UUID C = UUID.fromString("e0000003-0000-4000-8000-000000000003");
    private static final UUID D = UUID.fromString("e0000004-0000-4000-8000-000000000004");

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private final Map<UUID, ArticleResult> recorded = new ConcurrentHashMap<>();
    private final AtomicInteger reads = new AtomicInteger();
    // 몇 번째로 읽는지를 받아, 읽기 전에 처리 결과를 더할 수 있게 한다.
    private IntConsumer beforeRead = read -> { };
    private ArticleResultRepository results;
    private KafkaTemplate<String, String> kafka;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        results = mock(ArticleResultRepository.class);
        when(results.resultsOf(any())).thenAnswer(invocation -> {
            beforeRead.accept(reads.incrementAndGet());
            Collection<UUID> ids = invocation.getArgument(0);
            Map<UUID, ArticleResult> found = new ConcurrentHashMap<>();
            ids.stream().filter(recorded::containsKey).forEach(id -> found.put(id, recorded.get(id)));
            return found;
        });
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
    }

    // 이미 있는 기사(URL 중복)로 합친 것과 저장 실패도 처리가 끝난 것이므로, 모두 끝났으면 기다리지 않고 바로 낸다.
    @Test
    void publishesAtOnceWithCountsPerResultWhenEveryArticleIsProcessed() throws IOException {
        recorded.putAll(Map.of(A, ArticleResult.STORED, B, ArticleResult.STORED, C, ArticleResult.MERGED,
                D, ArticleResult.FAILED));

        notifier(Duration.ofMinutes(1)).onCompleted(completion(List.of(A, B, C, D), null));

        JsonNode event = mapper.readTree(storedJson());
        assertThat(ContractSchemas.validate(SCHEMA, storedJson())).isEmpty();
        assertThat(reads).hasValue(1);
        assertThat(event.get("eventType").asString()).isEqualTo("news.stored");
        assertThat(event.get("eventId").asString()).isEqualTo(StoredNotifier.storedEventId(COMPLETION_ID).toString());
        assertThat(event.get("correlationId").asString()).isEqualTo("collect-news-2026-10-20T16:15:00+09:00");
        JsonNode payload = event.get("payload");
        assertThat(payload.get("collectionEventId").asString()).isEqualTo(COMPLETION_ID);
        assertThat(payload.get("jobType").asString()).isEqualTo("collect-news");
        assertThat(payload.get("scheduledFor").asString()).isEqualTo("2026-10-20T16:15:00+09:00");
        assertThat(payload.get("startedAt").asString()).isEqualTo("2026-10-20T16:15:01+09:00");
        assertThat(texts(payload.get("incompleteSymbols"))).containsExactly("TEST02");
        assertThat(payload.has("researchJobId")).isFalse();
        assertThat(payload.get("articleCount").asInt()).isEqualTo(4);
        assertThat(payload.get("storedCount").asInt()).isEqualTo(2);
        assertThat(payload.get("mergedCount").asInt()).isEqualTo(1);
        assertThat(payload.get("failedCount").asInt()).isEqualTo(1);
        assertThat(payload.get("unprocessedEventIds")).isEmpty();
    }

    @Test
    void waitsUntilTheRemainingArticlesAreProcessed() throws IOException {
        recorded.put(A, ArticleResult.STORED);
        // 두 번째로 읽을 때 남은 기사가 처리돼 있다.
        beforeRead = read -> {
            if (read == 2) {
                recorded.put(B, ArticleResult.MERGED);
            }
        };

        notifier(Duration.ofMinutes(1)).onCompleted(completion(List.of(A, B), null));

        JsonNode payload = mapper.readTree(storedJson()).get("payload");
        assertThat(reads).hasValue(2);
        assertThat(payload.get("storedCount").asInt()).isEqualTo(1);
        assertThat(payload.get("mergedCount").asInt()).isEqualTo(1);
        assertThat(payload.get("unprocessedEventIds")).isEmpty();
    }

    @Test
    void publishesTheUnprocessedArticlesWhenTheWaitLimitPasses() throws IOException {
        recorded.put(A, ArticleResult.STORED);

        notifier(Duration.ofMillis(50)).onCompleted(completion(List.of(C, A, B), null));

        JsonNode payload = mapper.readTree(storedJson()).get("payload");
        assertThat(ContractSchemas.validate(SCHEMA, storedJson())).isEmpty();
        assertThat(reads.get()).isGreaterThan(1);
        assertThat(payload.get("articleCount").asInt()).isEqualTo(3);
        assertThat(payload.get("storedCount").asInt()).isEqualTo(1);
        assertThat(texts(payload.get("unprocessedEventIds"))).containsExactly(B.toString(), C.toString());
    }

    @Test
    void completionWithNoArticlesIsAnsweredAtOnce() throws IOException {
        notifier(Duration.ofMinutes(1)).onCompleted(completion(List.of(), null));

        JsonNode payload = mapper.readTree(storedJson()).get("payload");
        assertThat(payload.get("articleCount").asInt()).isZero();
        assertThat(payload.get("unprocessedEventIds")).isEmpty();
    }

    @Test
    void researchCompletionCarriesItsJobId() throws IOException {
        String jobId = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
        recorded.put(A, ArticleResult.STORED);

        notifier(Duration.ofMinutes(1)).onCompleted(completion(List.of(A), jobId)
                .replace("\"collect-news\"", "\"research-news\""));

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.NEWS_STORED), eq("research-news"), value.capture());
        assertThat(ContractSchemas.validate(SCHEMA, value.getValue())).isEmpty();
        assertThat(mapper.readTree(value.getValue()).get("payload").get("researchJobId").asString()).isEqualTo(jobId);
    }

    // 기사 목록 칸을 더하기 전의 수집기가 낸 완료에는 무엇을 기다릴지 몰라 내지 않는다.
    @Test
    void completionWithoutArticleEventIdsIsIgnored() throws IOException {
        String legacy = ContractSchemas.sample("collection.completed.sample.json")
                .replaceAll("(?s)    \"articleEventIds\": \\[.*?\\],\n", "");
        assertThat(legacy).doesNotContain("articleEventIds");

        notifier(Duration.ofMinutes(1)).onCompleted(legacy);

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void sameCompletionReceivedTwiceIsPublishedWithTheSameEventId() throws IOException {
        recorded.put(A, ArticleResult.STORED);
        StoredNotifier notifier = notifier(Duration.ofMinutes(1));

        notifier.onCompleted(completion(List.of(A), null));
        notifier.onCompleted(completion(List.of(A), null));

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.NEWS_STORED), eq("collect-news"), value.capture());
        assertThat(mapper.readTree(value.getAllValues().get(0)).get("eventId"))
                .isEqualTo(mapper.readTree(value.getAllValues().get(1)).get("eventId"));
    }

    // 전송 실패를 삼키면 수집 완료의 오프셋만 확정되고 알림은 사라진다. 던져서 오류 처리기가 다시 시도하게 한다.
    @Test
    void sendFailurePropagatesSoTheCompletionIsRetried() {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> notifier(Duration.ofMinutes(1)).onCompleted(completion(List.of(), null)))
                .hasRootCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        recorded.put(A, ArticleResult.STORED);
        String message = completion(List.of(A), null)
                .replace("\"correlationId\"", "\"futureEnvelopeField\": 1,\n  \"correlationId\"")
                .replace("\"jobType\"", "\"futurePayloadField\": {\"a\": 1},\n    \"jobType\"");

        notifier(Duration.ofMinutes(1)).onCompleted(message);

        verify(kafka, atLeast(1)).send(eq(Topics.NEWS_STORED), anyString(), anyString());
    }

    // 밀려 있다가 늦게 받은 완료는 기다려도 늦어질 뿐이라 지금 결과로 바로 낸다. 처리되지 않은 기사는 그대로 알린다.
    @Test
    void staleCompletionIsPublishedAtOnceWithTheCurrentResults() throws IOException {
        recorded.put(A, ArticleResult.STORED);

        notifier(Duration.ofMinutes(1)).onCompleted(staleCompletion(List.of(A, B), null));

        JsonNode payload = mapper.readTree(storedJson()).get("payload");
        assertThat(reads).hasValue(1);
        assertThat(payload.get("storedCount").asInt()).isEqualTo(1);
        assertThat(texts(payload.get("unprocessedEventIds"))).containsExactly(B.toString());
    }

    @Test
    void staleAfterDefaultsToTheWaitLimit() {
        assertThat(new StoredProperties(Duration.ofSeconds(60), Duration.ofSeconds(1), null).staleAfter())
                .isEqualTo(Duration.ofSeconds(60));
    }

    // 전송 실패로 같은 완료를 다시 처리할 때 또 기다리면 시도마다 한도만큼 늘어난다. 기다림은 처음 한 번뿐이다.
    @Test
    void retryOfTheSameCompletionDoesNotWaitAgain() throws IOException {
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")))
                .thenReturn(CompletableFuture.completedFuture(null));
        StoredNotifier notifier = notifier(Duration.ofMillis(50));
        String message = completion(List.of(A), null);

        assertThatThrownBy(() -> notifier.onCompleted(message)).hasRootCauseInstanceOf(IllegalStateException.class);
        int readsOfFirstTry = reads.get();
        notifier.onCompleted(message);

        assertThat(readsOfFirstTry).isGreaterThan(1);
        assertThat(reads.get() - readsOfFirstTry).isEqualTo(1);
    }

    // 기억하는 수에 상한이 있어 오래 돌아도 메모리가 늘지 않는다. 밀려난 완료를 다시 받으면 한 번 더 기다린다.
    @Test
    void waitedCompletionsAreForgottenBeyondTheCapacity() throws IOException {
        // 오래된 완료로 보지 않게 기준을 대기 한도보다 길게 둔다.
        StoredNotifier notifier = new StoredNotifier(results, kafka, mapper, Clock.system(ZoneId.of("Asia/Seoul")),
                new StoredProperties(Duration.ofMillis(30), Duration.ofMillis(5), Duration.ofMinutes(1)));
        String first = completion(List.of(A), null);
        notifier.onCompleted(first);
        for (int i = 0; i < StoredNotifier.WAITED_CAPACITY; i++) {
            notifier.onCompleted(staleCompletion(List.of(), null).replace(COMPLETION_ID, UUID.randomUUID().toString()));
        }
        int before = reads.get();

        notifier.onCompleted(first);

        assertThat(reads.get() - before).isGreaterThan(1);
    }

    // 오류 처리기는 실패한 완료를 되감아 다음 poll 에서 다시 받으므로 poll 간격 상한은 시도마다 따로 걸린다.
    // 한 시도의 최악(설정한 대기 한도 + 전송이 막히는 한도 + 전송 확정 한도)이 그 상한보다 짧아야 그룹에서 빠지지 않는다.
    @Test
    void oneAttemptAtWorstFitsInThePollIntervalLimit() throws IOException {
        StoredProperties configured = new Binder(ConfigurationPropertySources.from(new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"))))
                .bind("news.stored", StoredProperties.class).get();
        Map<String, Object> producer = ProducerConfig.configDef().defaultValues();
        Map<String, Object> consumer = ConsumerConfig.configDef().defaultValues();

        Duration worst = configured.waitLimit()
                .plusMillis(((Number) producer.get(ProducerConfig.MAX_BLOCK_MS_CONFIG)).longValue())
                .plusMillis(((Number) producer.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)).longValue());

        assertThat(worst).isLessThan(
                Duration.ofMillis(((Number) consumer.get(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG)).longValue()));
    }

    private StoredNotifier notifier(Duration waitLimit) {
        return new StoredNotifier(results, kafka, mapper, Clock.system(ZoneId.of("Asia/Seoul")),
                new StoredProperties(waitLimit, Duration.ofMillis(5), null));
    }

    private String storedJson() {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.NEWS_STORED), eq("collect-news"), value.capture());
        return value.getValue();
    }

    // 수집 완료 샘플에서 기사 목록과 재검색 작업 id 를 바꾸고, 수집 끝 시각을 지금으로 둔다(오래된 완료로 보지 않게).
    private static String completion(List<UUID> articleIds, String researchJobId) throws IOException {
        return staleCompletion(articleIds, researchJobId)
                .replace("\"finishedAt\": \"2026-10-20T16:15:42+09:00\"", "\"finishedAt\": \"" + OffsetDateTime.now() + "\"");
    }

    // 수집 끝 시각은 샘플 그대로(지난 날짜) 둔다.
    private static String staleCompletion(List<UUID> articleIds, String researchJobId) throws IOException {
        List<String> quoted = new ArrayList<>();
        articleIds.forEach(id -> quoted.add("\"" + id + "\""));
        String json = ContractSchemas.sample("collection.completed.sample.json")
                .replaceAll("(?s)\"articleEventIds\": \\[.*?\\]", "\"articleEventIds\": [" + String.join(", ", quoted) + "]");
        if (researchJobId != null) {
            json = json.replace("\"incompleteSymbols\": [\"TEST02\"]",
                    "\"incompleteSymbols\": [\"TEST02\"],\n    \"researchJobId\": \"" + researchJobId + "\"");
        }
        return json;
    }

    private static List<String> texts(JsonNode array) {
        List<String> values = new ArrayList<>();
        array.forEach(node -> values.add(node.asString()));
        return values;
    }
}

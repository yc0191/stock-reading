package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.marketdata.FailureType;
import dev.stockreading.collector.marketdata.FetchResult;
import dev.stockreading.collector.marketdata.MarketDataPort;
import dev.stockreading.collector.marketdata.PriceQuote;
import dev.stockreading.collector.marketdata.QuoteFailure;
import dev.stockreading.collector.marketdata.SimulatorMarketData;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TickListenerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final List<String> SYMBOLS = List.of("TEST01", "TEST02", "TEST03");

    private KafkaTemplate<String, String> kafka;
    private TickListener listener;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafka = mock(KafkaTemplate.class);
        ObjectMapper mapper = JsonMapper.builder().build();
        listener = new TickListener(
                new SimulatorMarketData(CLOCK), new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka, mapper, CLOCK);
    }

    @Test
    void publishesOneSnapshotPerSymbolKeyedBySymbolAndMatchingTheContract() throws IOException {
        listener.onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT), key.capture(), value.capture());

        assertThat(key.getAllValues()).containsExactlyElementsOf(SYMBOLS);
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", json)).isEmpty();
        }
    }

    @Test
    void ignoresDuplicateTickForTheSameScheduledTime() throws IOException {
        String tick = ContractSchemas.sample("schedule.tick.sample.json");

        listener.onTick(tick);
        listener.onTick(tick);

        verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
    }

    @Test
    void ignoresUnknownJobTypes() throws IOException {
        String tick = ContractSchemas.sample("schedule.tick.sample.json").replace("\"collect\"", "\"something-else\"");

        listener.onTick(tick);

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    private static final String SECRET_LIKE_RAW = "{\"dataHeader\":{\"resultCode\":\"9000\"},\"note\":\"server body\"}";

    private TickListener listenerWith(FetchResult result) {
        MarketDataPort port = new MarketDataPort() {
            @Override public String sourceName() { return "broker"; }
            @Override public FetchResult fetch(List<String> symbols) { return result; }
        };
        return new TickListener(port, new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka,
                JsonMapper.builder().build(), CLOCK);
    }

    @Test
    void snapshotCarriesTheRawResponse() throws IOException {
        var quote = new PriceQuote("TEST01", 1_000L, OffsetDateTime.parse("2026-10-20T16:00:00+09:00"), SECRET_LIKE_RAW);

        listenerWith(FetchResult.ofQuotes(List.of(quote))).onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.MARKET_SNAPSHOT), eq("TEST01"), value.capture());
        assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", value.getValue())).isEmpty();
        assertThat(JsonMapper.builder().build().readTree(value.getValue()).path("payload").path("raw").asString(""))
                .isEqualTo(SECRET_LIKE_RAW);
    }

    @Test
    void failuresArePublishedKeyedBySymbolWithTickCountsAndMatchTheContract() throws IOException {
        var quote = new PriceQuote("TEST01", 1_000L, OffsetDateTime.parse("2026-10-20T16:00:00+09:00"));
        var failure = new QuoteFailure("TEST02", FailureType.HTTP_ERROR, 500, "E777", 120L, null, 5_000L, 80_000L, 3,
                SECRET_LIKE_RAW);
        var skipped = QuoteFailure.skipped("TEST03", "RateLimited");

        listenerWith(new FetchResult(List.of(quote), List.of(failure, skipped)))
                .onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.MARKET_SNAPSHOT_FAILED), key.capture(), value.capture());
        assertThat(key.getAllValues()).containsExactly("TEST02", "TEST03");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", json)).isEmpty();
        }
        var first = JsonMapper.builder().build().readTree(value.getAllValues().get(0)).path("payload");
        assertThat(first.path("failureType").asString("")).isEqualTo("HTTP_ERROR");
        assertThat(first.path("tickSucceeded").asInt()).isEqualTo(1);
        assertThat(first.path("tickFailed").asInt()).isEqualTo(2);
        assertThat(first.path("responseRaw").asString("")).isEqualTo(SECRET_LIKE_RAW);
        var second = JsonMapper.builder().build().readTree(value.getAllValues().get(1)).path("payload");
        assertThat(second.path("failureType").asString("")).isEqualTo("TICK_SKIPPED");
        assertThat(second.path("exceptionType").asString("")).isEqualTo("RateLimited");
    }

    @Test
    void staleTickIsSkippedWithoutFetchingAndRecordedPerSymbol() throws IOException {
        MarketDataPort mustNotBeCalled = new MarketDataPort() {
            @Override public String sourceName() { return "broker"; }
            @Override public FetchResult fetch(List<String> symbols) { throw new AssertionError("조회하면 안 됩니다"); }
        };
        Clock threeSecondsLater = Clock.fixed(Instant.parse("2026-10-20T07:00:03Z"), ZoneId.of("Asia/Seoul"));
        var stale = new TickListener(mustNotBeCalled, new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka,
                JsonMapper.builder().build(), threeSecondsLater);

        stale.onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), value.capture());
        verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", json)).isEmpty();
            assertThat(json).contains("\"failureType\":\"TICK_SKIPPED\"");
        }
    }

    @Test
    void tickJustUnderTheAgeLimitIsProcessed() throws IOException {
        Clock almostStale = Clock.fixed(Instant.parse("2026-10-20T07:00:02.900Z"), ZoneId.of("Asia/Seoul"));
        var listener = new TickListener(new SimulatorMarketData(almostStale),
                new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka, JsonMapper.builder().build(), almostStale);

        listener.onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
    }

    private static String tickAt(String scheduledFor) throws IOException {
        return ContractSchemas.sample("schedule.tick.sample.json")
                .replace("\"scheduledFor\": \"2026-10-20T16:00:00+09:00\"", "\"scheduledFor\": \"" + scheduledFor + "\"")
                .replace("collect-2026-10-20T16:00:00+09:00", "collect-" + scheduledFor);
    }

    /** 조회한 횟수를 세는 시세 출처. 종목마다 성공 시세를 돌려준다. */
    private static final class CountingMarketData implements MarketDataPort {
        int fetches;

        @Override public String sourceName() { return "broker"; }

        @Override
        public FetchResult fetch(List<String> symbols) {
            fetches++;
            return FetchResult.ofQuotes(symbols.stream()
                    .map(s -> new PriceQuote(s, 1_000L, OffsetDateTime.parse("2026-10-20T16:00:00+09:00"))).toList());
        }
    }

    private List<JsonNode> sentPayloads(String topic, int times) throws IOException {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(times)).send(eq(topic), anyString(), value.capture());
        String schema = topic.equals(Topics.MARKET_SNAPSHOT)
                ? "market.snapshot.v1.schema.json" : "market.snapshot.failed.v1.schema.json";
        ObjectMapper mapper = JsonMapper.builder().build();
        List<JsonNode> events = new ArrayList<>();
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate(schema, json)).isEmpty();
            events.add(mapper.readTree(json));
        }
        return events;
    }

    @Test
    void piledUpTicksFetchOnlyTheLatestAndRecordTheOlderOnesAsSuperseded() throws IOException {
        var port = new CountingMarketData();
        Clock now = Clock.fixed(Instant.parse("2026-10-20T07:00:02.500Z"), ZoneId.of("Asia/Seoul"));
        var listener = new TickListener(port, new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka,
                JsonMapper.builder().build(), now);

        // 순서가 섞여 와도 scheduledFor 가 가장 늦은 tick 을 고른다.
        listener.onTicks(List.of(tickAt("2026-10-20T16:00:01+09:00"), tickAt("2026-10-20T16:00:02+09:00"),
                tickAt("2026-10-20T16:00:00+09:00")));

        assertThat(port.fetches).isEqualTo(1);
        assertThat(sentPayloads(Topics.MARKET_SNAPSHOT, 3))
                .allSatisfy(e -> assertThat(e.path("correlationId").asString())
                        .isEqualTo("collect-2026-10-20T16:00:02+09:00"));
        List<JsonNode> failures = sentPayloads(Topics.MARKET_SNAPSHOT_FAILED, 6);
        assertThat(failures).allSatisfy(e -> {
            assertThat(e.path("payload").path("failureType").asString()).isEqualTo("TICK_SKIPPED");
            assertThat(e.path("payload").path("exceptionType").asString()).isEqualTo(TickListener.SUPERSEDED);
            assertThat(e.path("payload").path("tickSucceeded").asInt()).isZero();
        });
        assertThat(failures).extracting(e -> e.path("payload").path("scheduledFor").asString())
                .containsOnly("2026-10-20T16:00:01+09:00", "2026-10-20T16:00:00+09:00");
        assertThat(failures).extracting(e -> e.path("payload").path("symbol").asString())
                .containsExactlyInAnyOrder("TEST01", "TEST02", "TEST03", "TEST01", "TEST02", "TEST03");
    }

    @Test
    void supersededSkipIsDistinctFromRateLimitedAndStaleSkips() {
        assertThat(TickListener.SUPERSEDED).isNotEqualTo("RateLimited").isNotBlank();
    }

    @Test
    void staleTicksInABatchKeepTheStaleRecordAndOnlyFreshOlderTicksAreSuperseded() throws IOException {
        var port = new CountingMarketData();
        Clock now = Clock.fixed(Instant.parse("2026-10-20T07:00:05.500Z"), ZoneId.of("Asia/Seoul"));
        var listener = new TickListener(port, new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka,
                JsonMapper.builder().build(), now);

        // 16:00:00 은 5.5초 밀려 상한 3초를 넘는다. 16:00:04 는 최신 tick 에 밀린다.
        listener.onTicks(List.of(tickAt("2026-10-20T16:00:00+09:00"), tickAt("2026-10-20T16:00:04+09:00"),
                tickAt("2026-10-20T16:00:05+09:00")));

        assertThat(port.fetches).isEqualTo(1);
        sentPayloads(Topics.MARKET_SNAPSHOT, 3);
        List<JsonNode> failures = sentPayloads(Topics.MARKET_SNAPSHOT_FAILED, 6);
        assertThat(failures).filteredOn(e -> e.path("payload").path("scheduledFor").asString()
                        .equals("2026-10-20T16:00:00+09:00"))
                .hasSize(3)
                .allSatisfy(e -> assertThat(e.path("payload").has("exceptionType")).isFalse());
        assertThat(failures).filteredOn(e -> e.path("payload").path("scheduledFor").asString()
                        .equals("2026-10-20T16:00:04+09:00"))
                .hasSize(3)
                .allSatisfy(e -> assertThat(e.path("payload").path("exceptionType").asString())
                        .isEqualTo(TickListener.SUPERSEDED));
    }

    @Test
    void duplicatesInsideABatchAndAcrossBatchesAreIgnoredWithoutEvents() throws IOException {
        var port = new CountingMarketData();
        Clock now = Clock.fixed(Instant.parse("2026-10-20T07:00:01.500Z"), ZoneId.of("Asia/Seoul"));
        var listener = new TickListener(port, new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka,
                JsonMapper.builder().build(), now);

        listener.onTicks(List.of(tickAt("2026-10-20T16:00:01+09:00"), tickAt("2026-10-20T16:00:01+09:00")));
        listener.onTicks(List.of(tickAt("2026-10-20T16:00:01+09:00")));

        assertThat(port.fetches).isEqualTo(1);
        sentPayloads(Topics.MARKET_SNAPSHOT, 3);
        verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), anyString());
    }

    @Test
    void otherJobTypesAndUnreadableMessagesInABatchDoNotStopTheCollectTick() throws IOException {
        var port = new CountingMarketData();
        var listener = new TickListener(port, new CollectorProperties(SYMBOLS, 1_000, 3_000), kafka,
                JsonMapper.builder().build(), CLOCK);
        String sample = ContractSchemas.sample("schedule.tick.sample.json");

        listener.onTicks(List.of("not json", sample.replace("\"collect\"", "\"collect-news\""), sample));

        // 다른 작업의 tick 은 최신 후보가 아니므로 collect tick 을 밀어내지 않는다.
        assertThat(port.fetches).isEqualTo(1);
        sentPayloads(Topics.MARKET_SNAPSHOT, 3);
        verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), anyString());
    }

    @Test
    void theKafkaListenerReceivesPolledTicksAsOneBatch() throws NoSuchMethodException {
        KafkaListener annotation = TickListener.class.getDeclaredMethod("onTicks", List.class)
                .getAnnotation(KafkaListener.class);

        assertThat(annotation.batch()).isEqualTo("true");
        assertThat(annotation.groupId()).isEqualTo("collector");
        assertThat(annotation.topics()).containsExactly(Topics.SCHEDULE_TICK);
    }

    @Test
    void aFailedSendDoesNotStopTheOtherSymbols() throws IOException {
        var quotes = List.of(
                new PriceQuote("TEST01", 1_000L, OffsetDateTime.parse("2026-10-20T16:00:00+09:00")),
                new PriceQuote("TEST02", 1_000L, OffsetDateTime.parse("2026-10-20T16:00:00+09:00")));
        doThrow(new IllegalStateException("kafka down")).when(kafka).send(eq(Topics.MARKET_SNAPSHOT), eq("TEST01"), anyString());

        listenerWith(FetchResult.ofQuotes(quotes)).onTick(ContractSchemas.sample("schedule.tick.sample.json"));

        verify(kafka).send(eq(Topics.MARKET_SNAPSHOT), eq("TEST02"), anyString());
    }
}

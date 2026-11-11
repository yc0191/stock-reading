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

class QuotePublisherTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final List<String> SYMBOLS = List.of("TEST01", "TEST02", "TEST03");
    private static final String AT = "2026-10-20T16:00:00+09:00";
    private static final String CORR = "collect-" + AT;
    private static final String SECRET_LIKE_RAW = "{\"dataHeader\":{\"resultCode\":\"9000\"},\"note\":\"server body\"}";

    private KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafka = mock(KafkaTemplate.class);
    }

    private QuotePublisher publisher(MarketDataPort port) {
        return new QuotePublisher(port, new CollectorProperties(SYMBOLS, 1_000), kafka, mapper, CLOCK);
    }

    private QuotePublisher publisherWith(FetchResult result) {
        return publisher(new MarketDataPort() {
            @Override public String sourceName() { return "broker"; }
            @Override public FetchResult fetch(List<String> symbols) { return result; }
        });
    }

    private List<JsonNode> sentEvents(String topic, int times) throws IOException {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(times)).send(eq(topic), anyString(), value.capture());
        String schema = topic.equals(Topics.MARKET_SNAPSHOT)
                ? "market.snapshot.v1.schema.json" : "market.snapshot.failed.v1.schema.json";
        List<JsonNode> events = new ArrayList<>();
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate(schema, json)).isEmpty();
            events.add(mapper.readTree(json));
        }
        return events;
    }

    @Test
    void publishesOneSnapshotPerSymbolKeyedBySymbolAndMatchingTheContract() throws IOException {
        publisher(new SimulatorMarketData(CLOCK)).publishCollected(CORR, AT);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactlyElementsOf(SYMBOLS);
        assertThat(sentEvents(Topics.MARKET_SNAPSHOT, 3))
                .allSatisfy(e -> assertThat(e.path("correlationId").asString()).isEqualTo(CORR));
    }

    @Test
    void snapshotCarriesTheRawResponse() throws IOException {
        var quote = new PriceQuote("TEST01", 1_000L, OffsetDateTime.parse(AT), SECRET_LIKE_RAW);

        publisherWith(FetchResult.ofQuotes(List.of(quote))).publishCollected(CORR, AT);

        JsonNode snapshot = sentEvents(Topics.MARKET_SNAPSHOT, 1).get(0);
        assertThat(snapshot.path("payload").path("raw").asString("")).isEqualTo(SECRET_LIKE_RAW);
    }

    @Test
    void failuresArePublishedKeyedBySymbolWithRoundCountsAndMatchTheContract() throws IOException {
        var quote = new PriceQuote("TEST01", 1_000L, OffsetDateTime.parse(AT));
        var failure = new QuoteFailure("TEST02", FailureType.HTTP_ERROR, 500, "E777", 120L, null, 5_000L, 80_000L, 3,
                SECRET_LIKE_RAW);
        var skipped = QuoteFailure.skipped("TEST03", "RateLimited");

        publisherWith(new FetchResult(List.of(quote), List.of(failure, skipped))).publishCollected(CORR, AT);

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.MARKET_SNAPSHOT_FAILED), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly("TEST02", "TEST03");
        List<JsonNode> failures = sentEvents(Topics.MARKET_SNAPSHOT_FAILED, 2);
        JsonNode first = failures.get(0).path("payload");
        assertThat(first.path("failureType").asString("")).isEqualTo("HTTP_ERROR");
        assertThat(first.path("scheduledFor").asString("")).isEqualTo(AT);
        assertThat(first.path("tickSucceeded").asInt()).isEqualTo(1);
        assertThat(first.path("tickFailed").asInt()).isEqualTo(2);
        assertThat(first.path("responseRaw").asString("")).isEqualTo(SECRET_LIKE_RAW);
        JsonNode second = failures.get(1).path("payload");
        assertThat(second.path("failureType").asString("")).isEqualTo("TICK_SKIPPED");
        assertThat(second.path("exceptionType").asString("")).isEqualTo("RateLimited");
    }

    @Test
    void aFailedSendDoesNotStopTheOtherSymbols() {
        var quotes = List.of(
                new PriceQuote("TEST01", 1_000L, OffsetDateTime.parse(AT)),
                new PriceQuote("TEST02", 1_000L, OffsetDateTime.parse(AT)));
        doThrow(new IllegalStateException("kafka down")).when(kafka).send(eq(Topics.MARKET_SNAPSHOT), eq("TEST01"), anyString());

        publisherWith(FetchResult.ofQuotes(quotes)).publishCollected(CORR, AT);

        verify(kafka).send(eq(Topics.MARKET_SNAPSHOT), eq("TEST02"), anyString());
    }

    @Test
    void skippedRoundIsRecordedPerSymbolAsSupersededWithoutFetching() throws IOException {
        MarketDataPort mustNotBeCalled = new MarketDataPort() {
            @Override public String sourceName() { return "broker"; }
            @Override public FetchResult fetch(List<String> symbols) { throw new AssertionError("조회하면 안 됩니다"); }
        };

        publisher(mustNotBeCalled).publishSkipped("collect-2026-10-20T16:00:01+09:00", "2026-10-20T16:00:01+09:00");

        verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
        List<JsonNode> failures = sentEvents(Topics.MARKET_SNAPSHOT_FAILED, 3);
        assertThat(failures).extracting(e -> e.path("payload").path("symbol").asString())
                .containsExactlyElementsOf(SYMBOLS);
        assertThat(failures).allSatisfy(e -> {
            assertThat(e.path("correlationId").asString()).isEqualTo("collect-2026-10-20T16:00:01+09:00");
            assertThat(e.path("payload").path("failureType").asString()).isEqualTo("TICK_SKIPPED");
            assertThat(e.path("payload").path("exceptionType").asString()).isEqualTo("SupersededByNewerTick");
            assertThat(e.path("payload").path("scheduledFor").asString()).isEqualTo("2026-10-20T16:00:01+09:00");
            assertThat(e.path("payload").path("tickSucceeded").asInt()).isZero();
            assertThat(e.path("payload").path("tickFailed").asInt()).isEqualTo(3);
        });
    }

    @Test
    void supersededSkipIsDistinctFromRateLimitedSkips() {
        // 데일리 메일 서버 상태가 exceptionType 으로 건너뜀 사유를 나눠 센다. 값이 바뀌면 그 집계가 조용히 틀어진다.
        assertThat(QuotePublisher.SUPERSEDED).isEqualTo("SupersededByNewerTick").isNotEqualTo("RateLimited");
    }
}

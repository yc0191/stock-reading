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
import dev.stockreading.collector.holdings.QuoteSymbolsPort;
import dev.stockreading.collector.marketdata.QuotePerTick;
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
        return publisher(port, SYMBOLS, 8);
    }

    private QuotePublisher publisher(MarketDataPort port, List<String> symbols, int perTick) {
        return new QuotePublisher(port, (QuoteSymbolsPort) () -> symbols, kafka, mapper, CLOCK,
                new CollectorProperties(1_000, perTick), new QuotePerTick(perTick));
    }

    /** 부른 종목을 회차별로 기록하는 가짜 시세 제공자. */
    private static final class RecordingPort implements MarketDataPort {
        final List<List<String>> calls = new ArrayList<>();
        @Override public String sourceName() { return "broker"; }
        @Override public FetchResult fetch(List<String> symbols) {
            calls.add(List.copyOf(symbols));
            return FetchResult.ofQuotes(symbols.stream()
                    .map(s -> new PriceQuote(s, 1_000L, OffsetDateTime.parse(AT))).toList());
        }
    }

    private static List<String> symbols(int count) {
        return java.util.stream.IntStream.rangeClosed(1, count).mapToObj(i -> String.format("TEST%02d", i)).toList();
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

    // 시세 종목이 비면 1초 회차마다 경고가 쏟아지지 않게, 처음 한 번과 그 뒤 10분마다만 남긴다.
    @Test
    void emptyQuoteSymbolsWarnOnceThenOnlyEveryTenMinutes() {
        var now = new java.util.concurrent.atomic.AtomicReference<>(Instant.parse("2026-10-20T07:00:00Z"));
        Clock clock = new Clock() {
            @Override public ZoneId getZone() { return ZoneId.of("Asia/Seoul"); }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.start();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(QuotePublisher.class);
        logger.addAppender(logs);
        try {
            QuotePublisher publisher = new QuotePublisher(new SimulatorMarketData(CLOCK),
                    (QuoteSymbolsPort) List::of, kafka, mapper, clock,
                    new CollectorProperties(1_000, 8), new QuotePerTick(8));

            for (int i = 0; i < 5; i++) {
                publisher.publishCollected(CORR, AT);
                now.set(now.get().plusSeconds(1));
            }
            assertThat(warnCount(logs)).isEqualTo(1);

            now.set(now.get().plus(QuotePublisher.EMPTY_WARN_INTERVAL));
            publisher.publishCollected(CORR, AT);
            assertThat(warnCount(logs)).isEqualTo(2);
        } finally {
            logger.detachAppender(logs);
        }
    }

    private static long warnCount(ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> logs) {
        return logs.list.stream().filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN).count();
    }

    @Test
    void elevenSymbolsWithEightPerTickRotateThroughTheListAndWrapAround() {
        var port = new RecordingPort();
        QuotePublisher publisher = publisher(port, symbols(11), 8);

        publisher.publishCollected(CORR, AT);
        publisher.publishCollected(CORR, AT);
        publisher.publishCollected(CORR, AT);

        assertThat(port.calls.get(0)).containsExactlyElementsOf(symbols(8));
        assertThat(port.calls.get(1)).containsExactly("TEST09", "TEST10", "TEST11", "TEST01", "TEST02", "TEST03", "TEST04", "TEST05");
        assertThat(port.calls.get(2)).containsExactly("TEST06", "TEST07", "TEST08", "TEST09", "TEST10", "TEST11", "TEST01", "TEST02");
    }

    @Test
    void concurrentRoundsKeepTheTurnWithoutDuplicatesOrGaps() throws Exception {
        // 11종목, 회차당 4: 조회 22번이면 88칸이라 종목마다 정확히 8번씩 불려야 한다. 건너뜀이 섞여도 차례는 그대로다.
        var fetched = new java.util.concurrent.ConcurrentLinkedQueue<String>();
        MarketDataPort port = new MarketDataPort() {
            @Override public String sourceName() { return "broker"; }
            @Override public FetchResult fetch(List<String> symbols) {
                fetched.addAll(symbols);
                return FetchResult.ofQuotes(List.of());
            }
        };
        QuotePublisher publisher = publisher(port, symbols(11), 4);
        int collected = 22;
        int skipped = 10;
        var pool = java.util.concurrent.Executors.newFixedThreadPool(8);
        var start = new java.util.concurrent.CountDownLatch(1);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < collected + skipped; i++) {
            boolean collect = i < collected;
            futures.add(pool.submit(() -> {
                start.await();
                if (collect) {
                    publisher.publishCollected(CORR, AT);
                } else {
                    publisher.publishSkipped(CORR, AT);
                }
                return null;
            }));
        }
        start.countDown();
        for (var future : futures) {
            future.get(10, java.util.concurrent.TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(fetched).hasSize(collected * 4);
        var counts = fetched.stream()
                .collect(java.util.stream.Collectors.groupingBy(s -> s, java.util.stream.Collectors.counting()));
        assertThat(counts).containsOnlyKeys(symbols(11)).allSatisfy((symbol, count) -> assertThat(count).isEqualTo(8L));
    }

    @Test
    void allSymbolsAreFetchedEveryRoundWhenNotMoreThanPerTick() {
        var port = new RecordingPort();
        QuotePublisher publisher = publisher(port, symbols(5), 8);

        publisher.publishCollected(CORR, AT);
        publisher.publishCollected(CORR, AT);

        assertThat(port.calls).containsExactly(symbols(5), symbols(5));
    }

    @Test
    void symbolCountEqualToPerTickFetchesEverythingEveryRound() {
        var port = new RecordingPort();
        QuotePublisher publisher = publisher(port, symbols(8), 8);

        publisher.publishCollected(CORR, AT);
        publisher.publishCollected(CORR, AT);

        assertThat(port.calls).containsExactly(symbols(8), symbols(8));
    }

    @Test
    void symbolCountNotMultipleOfPerTickKeepsEveryTurnFairOverACycle() {
        // 10종목, 회차당 4: 5회차(= 20칸)에 종목마다 정확히 두 번씩 불린다.
        var port = new RecordingPort();
        QuotePublisher publisher = publisher(port, symbols(10), 4);

        for (int i = 0; i < 5; i++) {
            publisher.publishCollected(CORR, AT);
        }

        var counts = port.calls.stream().flatMap(List::stream)
                .collect(java.util.stream.Collectors.groupingBy(s -> s, java.util.stream.Collectors.counting()));
        assertThat(counts).hasSize(10).allSatisfy((symbol, n) -> assertThat(n).isEqualTo(2));
    }

    @Test
    void symbolsOutsideTheTurnLeaveNoEventsAtAll() throws IOException {
        var port = new RecordingPort();

        publisher(port, symbols(11), 8).publishCollected(CORR, AT);

        verify(kafka, times(8)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
        verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), anyString());
    }

    @Test
    void skippedRoundRecordsOnlyTheTurnSymbolsAndDoesNotPassTheTurn() throws IOException {
        var port = new RecordingPort();
        QuotePublisher publisher = publisher(port, symbols(11), 8);

        publisher.publishCollected(CORR, AT);
        publisher.publishSkipped(CORR, AT);
        publisher.publishSkipped(CORR, AT);
        publisher.publishCollected(CORR, AT);

        // 건너뛴 두 회차는 둘째 회차의 차례(TEST09..TEST05)를 기록하고, 다음 실제 회차가 그 종목을 그대로 부른다.
        List<JsonNode> skipped = sentEvents(Topics.MARKET_SNAPSHOT_FAILED, 16);
        assertThat(skipped).extracting(e -> e.path("payload").path("symbol").asString())
                .containsExactlyElementsOf(java.util.stream.Stream.of(0, 1)
                        .flatMap(i -> java.util.stream.Stream.of("TEST09", "TEST10", "TEST11", "TEST01", "TEST02", "TEST03", "TEST04", "TEST05"))
                        .toList());
        assertThat(skipped).allSatisfy(e -> {
            assertThat(e.path("payload").path("exceptionType").asString()).isEqualTo("SupersededByNewerTick");
            assertThat(e.path("payload").path("tickFailed").asInt()).isEqualTo(8);
        });
        assertThat(port.calls.get(1)).containsExactly("TEST09", "TEST10", "TEST11", "TEST01", "TEST02", "TEST03", "TEST04", "TEST05");
    }

    @Test
    void aTurnSymbolDeniedByTheCallLimitIsStillRecordedAsRateLimited() throws IOException {
        var denied = QuoteFailure.skipped("TEST02", "RateLimited");
        MarketDataPort port = new MarketDataPort() {
            @Override public String sourceName() { return "broker"; }
            @Override public FetchResult fetch(List<String> symbols) {
                return new FetchResult(List.of(new PriceQuote("TEST01", 1_000L, OffsetDateTime.parse(AT))), List.of(denied));
            }
        };

        publisher(port, symbols(11), 8).publishCollected(CORR, AT);

        List<JsonNode> failures = sentEvents(Topics.MARKET_SNAPSHOT_FAILED, 1);
        assertThat(failures.get(0).path("payload").path("symbol").asString()).isEqualTo("TEST02");
        assertThat(failures.get(0).path("payload").path("exceptionType").asString()).isEqualTo("RateLimited");
    }

    @Test
    void startupLogTellsTheIntervalPerSymbol() {
        var logs = new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
        logs.start();
        var logger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(QuotePublisher.class);
        logger.addAppender(logs);
        try {
            publisher(new RecordingPort(), symbols(11), 8);
            assertThat(logs.list).anySatisfy(e -> assertThat(e.getFormattedMessage())
                    .contains("symbols=11").contains("perTick=8").contains("every 2000 ms"));
        } finally {
            logger.detachAppender(logs);
        }
    }

    @Test
    void supersededSkipIsDistinctFromRateLimitedSkips() {
        // 데일리 메일 서버 상태가 exceptionType 으로 건너뜀 사유를 나눠 센다. 값이 바뀌면 그 집계가 조용히 틀어진다.
        assertThat(QuotePublisher.SUPERSEDED).isEqualTo("SupersededByNewerTick").isNotEqualTo("RateLimited");
    }
}

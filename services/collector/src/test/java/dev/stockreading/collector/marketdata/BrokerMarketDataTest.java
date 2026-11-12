package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import tools.jackson.databind.json.JsonMapper;

class BrokerMarketDataTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T01:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final String KEY = "key-secret-value";
    private static final String SECRET = "app-secret-value";

    private FakeBrokerServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    private BrokerMarketData marketData(int maxCalls, int timeoutSeconds, List<String> symbols) throws IOException {
        return marketData(maxCalls, timeoutSeconds, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS, symbols);
    }

    private BrokerMarketData marketData(int maxCalls, int timeoutSeconds, int quoteTimeoutSeconds, List<String> symbols)
            throws IOException {
        server = new FakeBrokerServer();
        var broker = new BrokerProperties(server.baseUrl(), KEY, SECRET, timeoutSeconds, maxCalls, quoteTimeoutSeconds);
        var client = new BrokerClient(broker, JsonMapper.builder().build(), CLOCK, CallRateLimiter.perSecond(10));
        return new BrokerMarketData(client, broker,
                CallRateLimiter.perSecond(maxCalls), PriceChecks.fake());
    }

    @Test
    void sourceNameIsBroker() throws IOException {
        assertThat(marketData(10, 5, List.of("TEST01")).sourceName()).isEqualTo("broker");
    }

    @Test
    void fetchesSymbolsInParallelAndKeepsTheirOrder() throws IOException {
        var symbols = List.of("TEST01", "TEST02", "TEST03", "TEST04", "TEST05");
        var marketData = marketData(10, 5, symbols);
        server.priceDelayMs = 300;
        marketData.fetch(List.of("TEST01")); // 토큰을 먼저 발급해 둔다

        long start = System.nanoTime();
        FetchResult result = marketData.fetch(symbols);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(result.quotes()).extracting(PriceQuote::symbol).containsExactlyElementsOf(symbols);
        assertThat(result.failures()).isEmpty();
        assertThat(server.maxInFlight.get()).isGreaterThan(1);
        assertThat(elapsedMs).isLessThan(5 * 300L);
    }

    @Test
    void oneFailureDoesNotStopOtherSymbols() throws IOException {
        var marketData = marketData(10, 5, List.of("TEST01"));
        server.priceBody = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{\"now_prc\":\"\"}}";

        FetchResult failed = marketData.fetch(List.of("TEST01", "TEST02"));
        assertThat(failed.quotes()).isEmpty();
        assertThat(failed.failures()).extracting(QuoteFailure::symbol).containsExactly("TEST01", "TEST02");

        server.priceBody = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{\"now_prc\":\"1000\"}}";
        assertThat(marketData.fetch(List.of("TEST01", "TEST02")).quotes()).hasSize(2);
    }

    @Test
    void symbolsOverTheLimitAreRecordedAsRateLimitedNotDropped() throws IOException {
        var symbols = List.of("TEST01", "TEST02", "TEST03", "TEST04");
        var marketData = marketData(2, 5, symbols);

        FetchResult result = marketData.fetch(symbols);

        assertThat(result.quotes()).extracting(PriceQuote::symbol).containsExactly("TEST01", "TEST02");
        assertThat(result.failures()).extracting(QuoteFailure::symbol).containsExactly("TEST03", "TEST04");
        assertThat(result.failures()).allSatisfy(failure -> {
            assertThat(failure.type()).isEqualTo(FailureType.TICK_SKIPPED);
            assertThat(failure.exceptionType()).isEqualTo(BrokerMarketData.RATE_LIMITED);
        });
        assertThat(server.priceCalls).hasValue(2);
    }

    @Test
    void aSwappedInLimitDecidesWhetherTheProviderIsCalled() throws IOException {
        server = new FakeBrokerServer();
        var broker = new BrokerProperties(server.baseUrl(), KEY, SECRET, 5, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS);
        var client = new BrokerClient(broker, JsonMapper.builder().build(), CLOCK, CallRateLimiter.perSecond(10));
        CallRateLimit denyAll = maxWait -> false;
        var marketData = new BrokerMarketData(client, broker, denyAll, PriceChecks.fake());

        FetchResult result = marketData.fetch(List.of("TEST01"));

        assertThat(result.quotes()).isEmpty();
        assertThat(result.failures()).extracting(QuoteFailure::exceptionType).containsOnly(BrokerMarketData.RATE_LIMITED);
        assertThat(server.priceCalls).hasValue(0);
    }

    @Test
    void callsAcrossTicksStayWithinThePerSecondLimit() throws IOException {
        var marketData = marketData(2, 5, List.of("TEST01", "TEST02"));
        marketData.fetch(List.of("TEST01", "TEST02"));

        // 같은 1초 안의 두 번째 tick 은 자리가 나기까지 기다려야 하고, 기다릴 수 있는 한도를 넘으면 건너뜀으로 기록한다.
        FetchResult second = marketData.fetch(List.of("TEST01", "TEST02"));

        assertThat(second.quotes()).isEmpty();
        assertThat(second.failures()).extracting(QuoteFailure::exceptionType).containsOnly(BrokerMarketData.RATE_LIMITED);
        assertThat(server.priceCalls).hasValue(2);
    }

    @Test
    void failureCarriesDiagnosticFieldsAndRawResponse() throws IOException {
        var marketData = marketData(10, 5, List.of("TEST01"));
        marketData.fetch(List.of("TEST01")); // 토큰 발급
        server.priceStatus = 500;
        server.priceBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"E777\"},\"x\":1}";

        QuoteFailure failure = marketData.fetch(List.of("TEST01")).failures().get(0);

        assertThat(failure.type()).isEqualTo(FailureType.HTTP_ERROR);
        assertThat(failure.httpStatus()).isEqualTo(500);
        assertThat(failure.responseCode()).isEqualTo("E777");
        assertThat(failure.elapsedMs()).isNotNull().isGreaterThanOrEqualTo(0);
        assertThat(failure.tokenAgeMs()).isNotNull();
        assertThat(failure.tokenRemainingMs()).isPositive();
        assertThat(failure.concurrentCalls()).isGreaterThanOrEqualTo(1);
        assertThat(failure.responseRaw()).isEqualTo(server.priceBody);
    }

    @Test
    void slowResponseBecomesTimeoutFailure() throws IOException {
        var marketData = marketData(10, 5, 1, List.of("TEST01"));
        marketData.fetch(List.of("TEST01"));
        server.priceDelayMs = 2_000;

        QuoteFailure failure = marketData.fetch(List.of("TEST01")).failures().get(0);

        assertThat(failure.type()).isEqualTo(FailureType.TIMEOUT);
        assertThat(failure.exceptionType()).contains("Timeout");
    }

    @Test
    void unreachableServerBecomesConnectionFailure() throws IOException {
        var marketData = marketData(10, 5, List.of("TEST01"));
        marketData.fetch(List.of("TEST01"));
        server.close();

        QuoteFailure failure = marketData.fetch(List.of("TEST01")).failures().get(0);

        assertThat(failure.type()).isIn(FailureType.CONNECTION_FAILED, FailureType.TIMEOUT);
        assertThat(failure.exceptionType()).isNotBlank();
    }

    @Test
    void mismatchedPriceBecomesFieldMismatchFailureWhileOtherSymbolsStillPublish() throws IOException {
        var symbols = List.of("TEST01", "TEST02", "TEST03");
        var marketData = marketData(10, 5, symbols);
        String good = PriceChecks.body("9900", "10000", "100", "D", "-1.00", "10100", "9800");
        // 깨진 응답에 토큰 값이 섞여 와도 원문에서는 가려져야 한다.
        String broken = PriceChecks.body("12345", "10000", "100", "D", "-1.00", "10100", "9800")
                .replace("}}", ",\"echo\":\"tok-secret-1\"}}");
        server.priceBody = good;
        server.priceBodyBySymbol.put("TEST02", broken);

        FetchResult result = marketData.fetch(symbols);

        assertThat(result.quotes()).extracting(PriceQuote::symbol).containsExactly("TEST01", "TEST03");
        assertThat(result.failures()).hasSize(1);
        QuoteFailure failure = result.failures().get(0);
        assertThat(failure.symbol()).isEqualTo("TEST02");
        assertThat(failure.type()).isEqualTo(FailureType.FIELD_MISMATCH);
        assertThat(failure.exceptionType()).isEqualTo("FieldMismatch:CHANGE+RATE+HIGH_LOW");
        assertThat(failure.httpStatus()).isEqualTo(200);
        assertThat(failure.responseRaw()).isEqualTo(broken.replace("tok-secret-1", "***"));
        assertThat(failure.elapsedMs()).isNotNull();
    }

    @Test
    void comparisonFieldsThatCannotBeReadDoNotDropTheQuote() throws IOException {
        var marketData = marketData(10, 5, List.of("TEST01"));
        server.priceBody = PriceChecks.body("9900", "", "x", "D", "", "10100", "9800");

        FetchResult result = marketData.fetch(List.of("TEST01"));

        assertThat(result.quotes()).extracting(PriceQuote::price).containsExactly(9900L);
        assertThat(result.failures()).isEmpty();
    }

    @Test
    void rejectedQuoteLogsOnlyCheckNamesNotServerValues() throws IOException {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        root.addAppender(appender);
        try {
            var marketData = marketData(10, 5, List.of("TEST01"));
            server.priceBody = PriceChecks.body("12345", "10000", "100", "D", "-1.00", "10100", "9800");

            assertThat(marketData.fetch(List.of("TEST01")).failures()).hasSize(1);

            List<String> lines = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
                    .filter(line -> line.contains("FIELD_MISMATCH")).toList();
            assertThat(lines).hasSize(1);
            assertThat(lines.get(0)).contains("FieldMismatch:CHANGE+RATE+HIGH_LOW")
                    .doesNotContain("12345", "10000", "10100", "9800", "dataBody", PriceChecks.RATE);
        } finally {
            root.detachAppender(appender);
        }
    }
}

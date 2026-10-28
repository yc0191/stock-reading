package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.stockreading.collector.CollectorProperties;
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
        server = new FakeBrokerServer();
        var broker = new BrokerProperties(server.baseUrl(), KEY, SECRET, timeoutSeconds, maxCalls);
        var client = new BrokerClient(broker, JsonMapper.builder().build(), CLOCK);
        return new BrokerMarketData(client, new CollectorProperties(symbols, 1_000, 3_000), broker,
                CallRateLimiter.perSecond(maxCalls));
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
        var broker = new BrokerProperties(server.baseUrl(), KEY, SECRET, 5, 10);
        var client = new BrokerClient(broker, JsonMapper.builder().build(), CLOCK);
        CallRateLimit denyAll = maxWait -> false;
        var marketData = new BrokerMarketData(client, new CollectorProperties(List.of("TEST01"), 1_000, 3_000), broker, denyAll);

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
        var marketData = marketData(10, 1, List.of("TEST01"));
        marketData.fetch(List.of("TEST01"));
        server.priceDelayMs = 2_500;

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
}

package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.collector.holdings.QuoteSymbolsPort;
import tools.jackson.databind.json.JsonMapper;

class BrokerClientRawAndTokenTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T01:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final String KEY = "key-secret-value";
    private static final String SECRET = "app-secret-value";
    private static final String TOKEN = "tok-secret-1";

    private FakeBrokerServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    private BrokerClient client() throws IOException {
        server = new FakeBrokerServer();
        return new BrokerClient(new BrokerProperties(server.baseUrl(), KEY, SECRET, 5, 10),
                JsonMapper.builder().build(), CLOCK, CallRateLimiter.perSecond(10));
    }

    @Test
    void quoteKeepsResponseBodyAsReceivedExceptForTrim() throws IOException {
        var client = client();
        String body = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER
                + ",\"dataBody\":{\"now_prc\":\"  10250\",\"later_added_field\":\"x\",\"is_nm\":\"테스트\"}}";
        server.priceBody = "\n  " + body + "  \n";

        PriceQuote quote = client.currentPrice("TEST01");

        assertThat(quote.raw()).isEqualTo(body);
        assertThat(quote.price()).isEqualTo(10250L);
    }

    @Test
    void quoteRawNeverContainsTheTokenResponseOrTokenValue() throws IOException {
        var client = client();

        PriceQuote quote = client.currentPrice("TEST01");

        assertThat(quote.raw()).doesNotContain("access_token", TOKEN);
    }

    @Test
    void tokenValueEchoedInAQuoteResponseIsMasked() throws IOException {
        var client = client();
        server.priceBody = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER
                + ",\"dataBody\":{\"now_prc\":\"500\",\"echo\":\"" + TOKEN + "\"}}";

        assertThat(client.currentPrice("TEST01").raw()).doesNotContain(TOKEN).contains("***");
    }

    @Test
    void failureRawKeepsServerBodyButMasksKeysSecretsAndToken() throws IOException {
        var client = client();
        client.currentPrice("TEST01");
        server.priceStatus = 500;
        server.priceBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"E1\"},\"echo\":\"Bearer " + TOKEN
                + " " + KEY + " " + SECRET + "\"}";

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOfSatisfying(BrokerApiException.class, e -> {
                    assertThat(e.responseRaw()).contains("\"processCode\":\"E1\"")
                            .doesNotContain(TOKEN, KEY, SECRET);
                    assertThat(e.getMessage()).doesNotContain(TOKEN, KEY, SECRET);
                });
    }

    @Test
    void tokenIssueFailureIsTypedAndDoesNotLeakEvenWhenTheServerEchoesTheKey() throws IOException {
        var client = client();
        server.tokenStatus = 401;
        server.tokenBody = "{\"dataHeader\":{\"resultCode\":\"9999\",\"processCode\":\"E999\"},\"echo\":\"" + KEY + "\"}";

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOfSatisfying(BrokerApiException.class, e -> {
                    assertThat(e.failureType()).isEqualTo(FailureType.TOKEN_ISSUE_FAILED);
                    assertThat(e.httpStatus()).isEqualTo(401);
                    assertThat(e.responseRaw()).doesNotContain(KEY);
                    assertThat(e.getMessage()).doesNotContain(KEY, SECRET);
                });
        assertThat(server.priceCalls).hasValue(0);
    }

    @Test
    void failureTypesFollowTheResponseShape() throws IOException {
        var client = client();

        server.priceBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"E2\"}}";
        assertThat(failureOf(client)).isEqualTo(FailureType.RESULT_CODE_FAILED);

        server.priceBody = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{\"now_prc\":\"\"}}";
        assertThat(failureOf(client)).isEqualTo(FailureType.EMPTY_PRICE);

        server.priceBody = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{\"now_prc\":\"abc\"}}";
        assertThat(failureOf(client)).isEqualTo(FailureType.PRICE_PARSE_FAILED);

        server.priceStatus = 502;
        server.priceBody = "<html>bad gateway</html>";
        assertThat(failureOf(client)).isEqualTo(FailureType.HTTP_ERROR);
    }

    private FailureType failureOf(BrokerClient client) {
        try {
            client.currentPrice("TEST01");
        } catch (BrokerApiException e) {
            return e.failureType();
        }
        throw new AssertionError("실패해야 합니다");
    }

    @Test
    void parallelCallsIssueTheTokenOnlyOnce() throws Exception {
        var client = client();
        server.tokenDelayMs = 300;
        int threads = 20;
        var start = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<PriceQuote>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return client.currentPrice("TEST01");
                }));
            }
            start.countDown();
            for (Future<PriceQuote> future : futures) {
                assertThat(future.get().price()).isEqualTo(10250L);
            }
        }

        assertThat(server.tokenCalls).hasValue(1);
    }

    @Test
    void parallelCallsDoNotHammerTheServerAfterATokenFailure() throws Exception {
        var client = client();
        server.tokenStatus = 500;
        server.tokenBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"E5\"}}";
        server.tokenDelayMs = 200;
        int threads = 10;
        var start = new CountDownLatch(1);

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            List<Future<FailureType>> futures = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return failureOf(client);
                }));
            }
            start.countDown();
            for (Future<FailureType> future : futures) {
                assertThat(future.get()).isEqualTo(FailureType.TOKEN_ISSUE_FAILED);
            }
        }

        // 기다리던 호출들은 직전 실패를 그대로 받고, 재시도는 다음 tick 에 맡긴다.
        assertThat(server.tokenCalls.get()).isLessThan(threads);
    }

    @Test
    void secretsNeverAppearInLogsOrSerializedFailures() throws Exception {
        Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        root.addAppender(appender);
        try {
            server = new FakeBrokerServer();
            var broker = new BrokerProperties(server.baseUrl(), KEY, SECRET, 5, 10);
            var marketData = new BrokerMarketData(new BrokerClient(broker, JsonMapper.builder().build(), CLOCK, CallRateLimiter.perSecond(10)),
                    (QuoteSymbolsPort) () -> List.of("TEST01"), broker, CallRateLimiter.perSecond(10),
                    PriceChecks.fake());
            marketData.fetch(List.of("TEST01"));
            server.priceStatus = 500;
            server.priceBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"bad code!\","
                    + "\"processMessage\":\"server-supplied-text\"},\"echo\":\"Bearer " + TOKEN + " " + KEY + " " + SECRET + "\"}";

            FetchResult result = marketData.fetch(List.of("TEST01"));

            String serialized = JsonMapper.builder().build().writeValueAsString(result);
            assertThat(serialized).doesNotContain(TOKEN, KEY, SECRET, "Authorization");
            assertThat(result.failures().get(0).responseCode()).isEqualTo("invalid");
            assertThat(appender.list).isNotEmpty();
            for (ILoggingEvent event : appender.list) {
                String line = event.getFormattedMessage();
                assertThat(line).doesNotContain(TOKEN, KEY, SECRET, "server-supplied-text", "Authorization");
            }
        } finally {
            root.detachAppender(appender);
        }
    }
}

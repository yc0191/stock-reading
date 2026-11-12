package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import tools.jackson.databind.json.JsonMapper;

/** 토큰이 거부(HTTP 401)되면 토큰을 버리고 새로 발급해 한 번만 다시 시도하고, 그 일을 로그로 남긴다. */
class BrokerTokenRejectionTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-10T01:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final String KEY = "app-key-value";
    private static final String SECRET = "app-secret-value";

    private FakeBrokerServer server;
    private BrokerClient client;
    private final AtomicInteger limiterCalls = new AtomicInteger();
    private volatile boolean limiterOpen = true;
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final Logger clientLog = (Logger) LoggerFactory.getLogger(BrokerClient.class);

    @BeforeEach
    void start() throws IOException {
        server = new FakeBrokerServer();
        server.rotateTokens = true;
        client = new BrokerClient(new BrokerProperties(server.baseUrl(), KEY, SECRET, 5, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS),
                JsonMapper.builder().build(), CLOCK, maxWait -> {
                    limiterCalls.incrementAndGet();
                    return limiterOpen;
                });
        logs.start();
        clientLog.addAppender(logs);
    }

    @AfterEach
    void stop() {
        clientLog.detachAppender(logs);
        server.close();
    }

    private List<String> warnMessages() {
        return logs.list.stream().filter(e -> e.getLevel().isGreaterOrEqual(Level.WARN))
                .map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Test
    void rejectedTokenIsDiscardedReissuedAndTheCallRetriedOnce() {
        server.rejectAuthorizedCalls.set(1);

        PriceQuote quote = client.currentPrice("TEST01");

        assertThat(quote.price()).isEqualTo(10250L);
        assertThat(server.tokenCalls).hasValue(2);
        assertThat(server.priceCalls).hasValue(2);
        // 두 번째 요청은 재발급된 토큰으로 갔다.
        assertThat(server.authHeaders).containsExactly("Bearer tok-secret-1", "Bearer tok-secret-2");
    }

    @Test
    void rejectionReissueAndRetryResultAreLoggedWithoutTokenOrKeys() {
        server.rejectAuthorizedCalls.set(1);

        client.currentPrice("TEST01");

        assertThat(warnMessages()).hasSize(2);
        assertThat(warnMessages().get(0)).contains("token rejected").contains("reissuing");
        assertThat(warnMessages().get(1)).contains("succeeded after token reissue");
        assertThat(String.join("\n", warnMessages())).doesNotContain("tok-secret", KEY, SECRET);
    }

    @Test
    void secondRejectionIsNotRetriedAgainAndIsLogged() {
        server.rejectAuthorizedCalls.set(2);

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOfSatisfying(BrokerApiException.class, e -> {
                    assertThat(e.failureType()).isEqualTo(FailureType.HTTP_ERROR);
                    assertThat(e.httpStatus()).isEqualTo(401);
                });

        assertThat(server.tokenCalls).hasValue(2);
        assertThat(server.priceCalls).hasValue(2);
        assertThat(warnMessages()).anyMatch(m -> m.contains("failed again after token reissue") && m.contains("http=401"));
        assertThat(String.join("\n", warnMessages())).doesNotContain("tok-secret", KEY, SECRET);
    }

    @Test
    void theRetryTakesACallSlotFromTheSharedLimit() {
        server.rejectAuthorizedCalls.set(1);

        client.currentPrice("TEST01");

        // 첫 호출의 칸은 호출하는 쪽이 받으므로 클라이언트가 받는 것은 재시도 한 칸뿐이다.
        assertThat(limiterCalls).hasValue(1);
    }

    @Test
    void withoutACallSlotTheRetryIsSkippedTheOriginal401IsRaisedAndTheTokenStaysDiscarded() {
        server.rejectAuthorizedCalls.set(1);
        limiterOpen = false;

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOfSatisfying(BrokerApiException.class, e -> assertThat(e.httpStatus()).isEqualTo(401));

        assertThat(server.priceCalls).hasValue(1);
        assertThat(server.tokenCalls).hasValue(1);
        assertThat(warnMessages()).hasSize(1);
        assertThat(warnMessages().getFirst()).contains("token rejected").contains("no call slot");

        // 토큰은 이미 버려졌으므로 다음 호출은 새 토큰으로 시작한다.
        client.currentPrice("TEST01");
        assertThat(server.tokenCalls).hasValue(2);
        assertThat(server.authHeaders).containsExactly("Bearer tok-secret-1", "Bearer tok-secret-2");
    }

    @Test
    void otherFailuresDoNotDiscardTheTokenOrRetry() {
        server.priceStatus = 500;

        assertThatThrownBy(() -> client.currentPrice("TEST01")).isInstanceOf(BrokerApiException.class);

        assertThat(server.tokenCalls).hasValue(1);
        assertThat(server.priceCalls).hasValue(1);
        assertThat(warnMessages()).isEmpty();
    }

    @Test
    void rawQueriesAlsoReissueTheTokenOnRejection() {
        server.rejectAuthorizedCalls.set(1);

        String raw = client.marketStatusRaw(FakeBrokerServer.MARKET_STATUS_PATH, Duration.ofSeconds(5));

        assertThat(raw).contains("today_field");
        assertThat(server.tokenCalls).hasValue(2);
        assertThat(server.marketStatusCalls).hasValue(2);
        assertThat(server.authHeaders).containsExactly("Bearer tok-secret-1", "Bearer tok-secret-2");
    }

    @Test
    void reissuedTokenIsReusedByLaterCalls() {
        server.rejectAuthorizedCalls.set(1);
        client.currentPrice("TEST01");

        client.currentPrice("TEST02");

        assertThat(server.tokenCalls).hasValue(2);
        assertThat(server.authHeaders).last().isEqualTo("Bearer tok-secret-2");
    }

    @Test
    void concurrentRejectionsOfTheSameTokenReissueItOnlyOnce() throws Exception {
        client.currentPrice("TEST01");
        int threads = 8;
        server.rejectOnlyBearer = "Bearer tok-secret-1";
        server.rejectAuthorizedCalls.set(1_000);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        try {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<PriceQuote>>();
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> client.currentPrice("TEST01")));
            }
            for (var future : futures) {
                assertThat(future.get().price()).isEqualTo(10250L);
            }
        } finally {
            pool.shutdownNow();
        }

        // 처음 발급 1회와, 거부된 첫 토큰을 한 번만 버리고 받은 재발급 1회뿐이다.
        assertThat(server.tokenCalls).hasValue(2);
    }
}

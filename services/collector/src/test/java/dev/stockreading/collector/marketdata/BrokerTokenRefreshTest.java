package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/** 토큰을 다시 발급할 시각: 평소에는 만료 5분 전, 남은 시간이 5분보다 짧게 받은 토큰은 실제 만료 시각이다. */
class BrokerTokenRefreshTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /** 시각을 테스트가 옮기는 시계. */
    private static final class MovableClock extends Clock {
        private Instant now = Instant.parse("2026-11-10T01:00:00Z");

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return KST;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(now, zone);
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private FakeBrokerServer server;
    private MovableClock clock;
    private BrokerClient client;

    @BeforeEach
    void start() throws IOException {
        server = new FakeBrokerServer();
        server.rotateTokens = true;
        clock = new MovableClock();
        client = new BrokerClient(new BrokerProperties(server.baseUrl(), "app-key-value", "app-secret-value", 5, 10,
                BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS), JsonMapper.builder().build(), clock, maxWait -> true);
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private void expiresIn(int seconds) {
        server.tokenBody = server.tokenBody.replace("86400", String.valueOf(seconds));
    }

    @Test
    void aTokenIssuedWithLessThanTheMarginLeftIsReusedUntilItExpires() {
        // 서버가 만료 5분 안의 토큰을 다시 돌려주는 상황(120초 남음).
        expiresIn(120);

        for (int i = 0; i < 5; i++) {
            client.currentPrice("TEST01");
        }

        assertThat(server.tokenCalls).hasValue(1);
    }

    @Test
    void aShortLivedTokenIsReissuedOnceAfterItExpires() {
        expiresIn(120);
        client.currentPrice("TEST01");

        clock.advance(Duration.ofSeconds(119));
        client.currentPrice("TEST01");
        assertThat(server.tokenCalls).hasValue(1);

        clock.advance(Duration.ofSeconds(1));
        client.currentPrice("TEST01");
        client.currentPrice("TEST02");
        client.currentPrice("TEST03");

        assertThat(server.tokenCalls).hasValue(2);
    }

    @Test
    void aTokenRejectedAtTheExpiryBoundaryIsDiscardedAndReissuedOnce() {
        expiresIn(120);
        client.currentPrice("TEST01");
        clock.advance(Duration.ofSeconds(119));
        server.rejectAuthorizedCalls.set(1);

        PriceQuote quote = client.currentPrice("TEST01");

        assertThat(quote.price()).isEqualTo(10250L);
        assertThat(server.tokenCalls).hasValue(2);
        assertThat(server.authHeaders).last().isEqualTo("Bearer tok-secret-2");
    }

    @Test
    void aNormalTokenIsStillReissuedFiveMinutesBeforeItExpires() {
        // 기본 만료는 86400초다.
        client.currentPrice("TEST01");

        clock.advance(Duration.ofSeconds(86400 - 301));
        client.currentPrice("TEST01");
        assertThat(server.tokenCalls).hasValue(1);

        clock.advance(Duration.ofSeconds(1));
        client.currentPrice("TEST01");
        client.currentPrice("TEST02");

        assertThat(server.tokenCalls).hasValue(2);
    }
}

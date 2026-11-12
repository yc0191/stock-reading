package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

/** 시세 클라이언트가 HTTP/1.1로 요청하는지, 현재가만 짧은 시간 제한(quote-timeout-seconds)으로 끊기는지 확인한다. */
class BrokerClientTimeoutTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T01:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final String OK_HEADER = "{\"resultCode\":\"200\",\"processCode\":\"0024\",\"processTime\":\"20261022101530123\"}";
    private static final String TOKEN_BODY =
            "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"access_token\":\"tok-1\",\"expires_in\":86400}}";
    private static final String PRICE_BODY = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"now_prc\":\"10250\"}}";
    private static final String RAW_BODY = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{}}";

    @Configuration
    @EnableConfigurationProperties(BrokerProperties.class)
    static class Wiring {
    }

    private HttpServer server;
    private final List<String> protocols = new CopyOnWriteArrayList<>();
    private final List<String> upgradeHeaders = new CopyOnWriteArrayList<>();
    private volatile long tokenDelayMs;
    private volatile long priceDelayMs;
    private volatile long otherDelayMs;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private BrokerClient client(int timeoutSeconds, int quoteTimeoutSeconds) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/oauth2/token", exchange -> reply(exchange, tokenDelayMs, TOKEN_BODY));
        server.createContext("/api/v1/ivu10140", exchange -> reply(exchange, priceDelayMs, PRICE_BODY));
        server.createContext("/api/v1/iva60190", exchange -> reply(exchange, otherDelayMs, RAW_BODY));
        server.start();
        var properties = new BrokerProperties("http://127.0.0.1:" + server.getAddress().getPort(), "ak", "as",
                timeoutSeconds, 10, quoteTimeoutSeconds);
        return new BrokerClient(properties, JsonMapper.builder().build(), CLOCK, CallRateLimiter.perSecond(10));
    }

    private void reply(HttpExchange exchange, long delayMs, String body) throws IOException {
        protocols.add(exchange.getProtocol());
        upgradeHeaders.add(String.valueOf(exchange.getRequestHeaders().getFirst("Upgrade")));
        exchange.getRequestBody().readAllBytes();
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        try {
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (IOException ignored) {
            // 클라이언트가 이미 끊은 요청이다.
        } finally {
            exchange.close();
        }
    }

    @Test
    void requestsAreSentAsHttp11WithoutUpgrade() throws IOException {
        var client = client(5, 2);

        client.currentPrice("TEST01");
        client.exchangeRateSummaryRaw();

        assertThat(protocols).isNotEmpty().containsOnly("HTTP/1.1");
        // 기본 클라이언트는 평문 연결에서 h2c 업그레이드 헤더를 붙인다. 테스트 서버는 평문이라 업그레이드 헤더로 확인한다(운영은 HTTPS에서 TLS 협상).
        assertThat(upgradeHeaders).containsOnly("null");
    }

    @Test
    void currentPriceIsCutOffByTheQuoteTimeoutAsTimeout() throws IOException {
        var client = client(5, 1);
        client.currentPrice("TEST01"); // 토큰을 먼저 발급해 둔다
        priceDelayMs = 1_600;

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOfSatisfying(BrokerApiException.class,
                        e -> assertThat(e.failureType()).isEqualTo(FailureType.TIMEOUT));
    }

    @Test
    void httpClientUsesHttp11AndTheQuoteTimeoutForConnecting() throws IOException {
        var client = client(10, 1);

        assertThat(client.httpClient().version()).isEqualTo(HttpClient.Version.HTTP_1_1);
        assertThat(client.httpClient().connectTimeout()).contains(Duration.ofSeconds(1));
    }

    @Test
    void tokenIssueAndOtherLookupsKeepTheLongerTimeout() throws IOException {
        var client = client(5, 1);
        tokenDelayMs = 1_600;
        otherDelayMs = 1_600;

        // 현재가 제한(1초)보다 느려도 토큰 발급과 그 밖의 조회는 끊기지 않는다.
        assertThat(client.currentPrice("TEST01").price()).isEqualTo(10250L);
        assertThat(client.exchangeRateSummaryRaw()).isEqualTo(RAW_BODY);
    }

    @Test
    void quoteTimeoutIsReadFromTheEnvironmentVariableNameAndDefaultsToTwoSeconds() {
        runner().run(context -> assertThat(context.getBean(BrokerProperties.class).quoteTimeoutSeconds()).isEqualTo(2));
        runner().withPropertyValues("BROKER_QUOTE_TIMEOUT_SECONDS=4").run(context ->
                assertThat(context.getBean(BrokerProperties.class).quoteTimeoutSeconds()).isEqualTo(4));
        runner().withPropertyValues("BROKER_QUOTE_TIMEOUT_SECONDS=0").run(context ->
                assertThat(context.getBean(BrokerProperties.class).quoteTimeoutSeconds()).isEqualTo(2));
    }

    @Test
    void generalTimeoutStaysSeparateFromTheQuoteTimeout() {
        runner().run(context -> assertThat(context.getBean(BrokerProperties.class).timeoutSeconds()).isEqualTo(10));
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }
}

package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

class BrokerClientTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final String OK_HEADER =
            "{\"resultCode\":\"200\",\"resultMessage\":\"성공\",\"processCode\":\"0024\",\"processTime\":\"20261022101530123\"}";

    private HttpServer server;
    private final AtomicInteger tokenCalls = new AtomicInteger();
    private final List<String> authHeaders = new ArrayList<>();
    private final List<String> priceBodies = new ArrayList<>();
    private String tokenBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"access_token\":\"tok-1\",\"token_type\":\"Bearer\",\"expires_in\":86400}}";
    private int tokenStatus = 200;
    private String priceBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"now_prc\":\"  10250\",\"is_nm\":\"테스트\"}}";

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private BrokerClient client(Clock clock, String appKey, String appSecret) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth2/token", exchange -> {
            tokenCalls.incrementAndGet();
            reply(exchange, tokenStatus, tokenBody);
        });
        server.createContext("/api/v1/ivu10140", exchange -> {
            authHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            priceBodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            reply(exchange, 200, priceBody);
        });
        server.start();
        var properties = new BrokerProperties("http://127.0.0.1:" + server.getAddress().getPort(), appKey, appSecret, 5);
        return new BrokerClient(properties, JsonMapper.builder().build(), clock);
    }

    private static void reply(com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static Clock clockAt(String instant) {
        return Clock.fixed(Instant.parse(instant), KST);
    }

    @Test
    void parsesPriceAndProcessTimeInKst() throws IOException {
        var client = client(clockAt("2026-10-22T01:00:00Z"), "ak", "as");

        PriceQuote quote = client.currentPrice("TEST01");

        assertThat(quote.symbol()).isEqualTo("TEST01");
        assertThat(quote.price()).isEqualTo(10250L);
        assertThat(quote.asOf().toString()).isEqualTo("2026-10-22T10:15:30+09:00");
        assertThat(quote.asOf().getOffset()).isEqualTo(ZoneOffset.ofHours(9));
        assertThat(priceBodies.get(0)).contains("\"shrt_cd\":\"TEST01\"", "\"excg_clsf\":\"0\"");
        assertThat(authHeaders.get(0)).isEqualTo("Bearer tok-1");
    }

    @Test
    void reusesTokenUntilItIsNearExpiry() throws IOException {
        var client = client(clockAt("2026-10-22T01:00:00Z"), "ak", "as");

        client.currentPrice("TEST01");
        client.currentPrice("TEST02");

        assertThat(tokenCalls).hasValue(1);
    }

    @Test
    void reissuesTokenAfterExpiry() throws IOException {
        tokenBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"access_token\":\"tok-1\",\"token_type\":\"Bearer\",\"expires_in\":600}}";
        var now = new Instant[] {Instant.parse("2026-10-22T01:00:00Z")};
        Clock movable = new Clock() {
            @Override public ZoneId getZone() { return KST; }
            @Override public Clock withZone(ZoneId zone) { return this; }
            @Override public Instant instant() { return now[0]; }
        };
        var client = client(movable, "ak", "as");

        client.currentPrice("TEST01");
        now[0] = now[0].plusSeconds(400); // 만료 5분 전 여유(600-300=300초)를 넘긴다
        client.currentPrice("TEST01");

        assertThat(tokenCalls).hasValue(2);
    }

    @Test
    void errorHeaderThrowsWithoutLeakingSecrets() throws IOException {
        tokenStatus = 500;
        tokenBody = "{\"dataHeader\":{\"resultCode\":\"9999\",\"processCode\":\"E999\",\"processMessage\":\"앱 정보를 찾지 못했습니다.\"}}";
        var client = client(clockAt("2026-10-22T01:00:00Z"), "secret-key", "secret-secret");

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOf(BrokerApiException.class)
                .hasMessageContaining("E999")
                .hasMessageNotContaining("secret-key")
                .hasMessageNotContaining("secret-secret");
        assertThat(priceBodies).isEmpty();
    }

    @Test
    void missingCredentialsFailBeforeAnyCall() throws IOException {
        var client = client(clockAt("2026-10-22T01:00:00Z"), "", "");

        assertThatThrownBy(() -> client.currentPrice("TEST01")).isInstanceOf(BrokerApiException.class);
        assertThat(tokenCalls).hasValue(0);
    }

    @Test
    void zeroOrUnparsablePriceIsRejected() throws IOException {
        var client = client(clockAt("2026-10-22T01:00:00Z"), "ak", "as");

        priceBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"now_prc\":\"0\"}}";
        assertThatThrownBy(() -> client.currentPrice("TEST01")).isInstanceOf(BrokerApiException.class);

        priceBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"now_prc\":\"abc\"}}";
        assertThatThrownBy(() -> client.currentPrice("TEST01")).isInstanceOf(BrokerApiException.class);
    }

    @Test
    void missingProcessTimeFallsBackToClock() throws IOException {
        priceBody = "{\"dataHeader\":{\"resultCode\":\"200\"},\"dataBody\":{\"now_prc\":\"500\"}}";
        var client = client(clockAt("2026-10-22T01:00:00Z"), "ak", "as");

        assertThat(client.currentPrice("TEST01").asOf().toString()).isEqualTo("2026-10-22T10:00+09:00");
    }

    @Test
    void propertiesToStringHidesSecrets() {
        assertThat(new BrokerProperties("u", "secret-key", "secret-secret", 5).toString())
                .doesNotContain("secret-key", "secret-secret");
    }
}

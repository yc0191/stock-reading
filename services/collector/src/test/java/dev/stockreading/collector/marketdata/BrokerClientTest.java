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
        var properties = new BrokerProperties("http://127.0.0.1:" + server.getAddress().getPort(), appKey, appSecret, 5, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS);
        return new BrokerClient(properties, JsonMapper.builder().build(), clock, CallRateLimiter.perSecond(10));
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
    void serverErrorKeepsProcessCodeButNotServerMessage() throws IOException {
        tokenStatus = 500;
        tokenBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"E777\",\"processMessage\":\"테스트용 서버 오류 문구\"}}";
        var client = client(clockAt("2026-10-22T01:00:00Z"), "ak", "as");

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOf(BrokerApiException.class)
                .hasMessageContaining("http=500")
                .hasMessageContaining("processCode=E777")
                .hasMessageNotContaining("테스트용 서버 오류 문구");
    }

    @Test
    void serverErrorWithMalformedProcessCodeIsNotEchoed() throws IOException {
        tokenStatus = 500;
        tokenBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"bad code!\"}}";
        var client = client(clockAt("2026-10-22T01:00:00Z"), "ak", "as");

        assertThatThrownBy(() -> client.currentPrice("TEST01"))
                .isInstanceOf(BrokerApiException.class)
                .hasMessageContaining("processCode=invalid")
                .hasMessageNotContaining("bad code");
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

    // 실제 응답의 형태(헤더 코드, 공백이 붙은 숫자 필드, 모르는 필드)만 본떠 가짜 값으로 만든다.
    private static String priceResponse(String header, String body) {
        return "{\"dataHeader\":" + header + ",\"dataBody\":" + body + "}";
    }

    @Test
    void ignoresUnknownAndPaddedFields() throws IOException {
        priceBody = priceResponse(OK_HEADER, "{\"now_prc\":\"10250\",\"up_dwn_r_p2\":\"   -0.50\","
                + "\"expct_ccls_q\":\"  12\",\"later_added_field\":\"x\"}");
        var client = client(clockAt("2026-10-26T06:00:00Z"), "ak", "as");

        assertThat(client.currentPrice("TEST01").price()).isEqualTo(10250L);
    }

    @Test
    void afterCloseResponseUsesProcessTimeAsAsOf() throws IOException {
        // 장 마감 후에도 성공 코드와 구조는 장중과 같고, 시세 시각이 없어 처리 시각이 asOf 가 된다.
        String afterClose = "{\"resultCode\":\"200\",\"processCode\":\"0001\",\"processTime\":\"20260102153501123\"}";
        priceBody = priceResponse(afterClose, "{\"now_prc\":\"12345\"}");
        var client = client(clockAt("2026-10-26T06:50:00Z"), "ak", "as");

        PriceQuote quote = client.currentPrice("TEST01");

        assertThat(quote.price()).isEqualTo(12345L);
        assertThat(quote.asOf().toString()).isEqualTo("2026-01-02T15:35:01+09:00");
    }

    @Test
    void emptyPriceFailureKeepsProcessCodeButNotServerMessage() throws IOException {
        // 없는 종목은 HTTP 200, resultCode 200 으로 오고 본문 값이 모두 비어 있다.
        String header = "{\"resultCode\":\"200\",\"processCode\":\"0999\",\"processMessage\":\"테스트용 조회 오류\","
                + "\"processTime\":\"20260102101010101\"}";
        priceBody = priceResponse(header, "{\"now_prc\":\"\",\"is_nm\":\"\"}");
        var client = client(clockAt("2026-10-26T06:00:00Z"), "ak", "as");

        assertThatThrownBy(() -> client.currentPrice("TEST99"))
                .isInstanceOf(BrokerApiException.class)
                .hasMessageContaining("TEST99")
                .hasMessageContaining("processCode=0999")
                .hasMessageNotContaining("테스트용 조회 오류")
                .hasMessageNotContaining("tok-1");
    }

    @Test
    void malformedProcessCodeIsNotEchoed() throws IOException {
        String header = "{\"resultCode\":\"200\",\"processCode\":\"<b>evil</b>\\nline\"}";
        priceBody = priceResponse(header, "{\"now_prc\":\"\"}");
        var client = client(clockAt("2026-10-26T06:00:00Z"), "ak", "as");

        assertThatThrownBy(() -> client.currentPrice("TEST99"))
                .isInstanceOf(BrokerApiException.class)
                .hasMessageContaining("processCode=invalid")
                .hasMessageNotContaining("evil");
    }

    @Test
    void missingProcessTimeFallsBackToClock() throws IOException {
        priceBody = "{\"dataHeader\":{\"resultCode\":\"200\"},\"dataBody\":{\"now_prc\":\"500\"}}";
        var client = client(clockAt("2026-10-22T01:00:00Z"), "ak", "as");

        assertThat(client.currentPrice("TEST01").asOf().toString()).isEqualTo("2026-10-22T10:00+09:00");
    }

    @Test
    void propertiesToStringHidesSecrets() {
        assertThat(new BrokerProperties("u", "secret-key", "secret-secret", 5, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS).toString())
                .doesNotContain("secret-key", "secret-secret");
    }
}

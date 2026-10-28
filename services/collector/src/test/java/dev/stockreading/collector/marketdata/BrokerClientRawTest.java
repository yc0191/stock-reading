package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

/** 마감 후 조회용 원문 메서드 검증. 응답은 모양만 본뜬 가짜 값이다. */
class BrokerClientRawTest {

    private static final String OK_HEADER = "{\"resultCode\":\"200\",\"processCode\":\"0024\"}";

    private HttpServer server;
    private BrokerClient client;
    private final Map<String, String> requestBodies = new LinkedHashMap<>();
    private final Map<String, String> authHeaders = new LinkedHashMap<>();
    private int apiStatus = 200;
    private String apiBody;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/oauth2/token", exchange -> reply(exchange, 200,
                "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"access_token\":\"tok-1\",\"expires_in\":86400}}"));
        server.createContext("/api/v1/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            requestBodies.put(path, new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            authHeaders.put(path, exchange.getRequestHeaders().getFirst("Authorization"));
            reply(exchange, apiStatus, apiBody);
        });
        server.start();
        var properties = new BrokerProperties("http://127.0.0.1:" + server.getAddress().getPort(), "ak", "as", 5, 10);
        client = new BrokerClient(properties, JsonMapper.builder().build(),
                Clock.fixed(Instant.parse("2026-10-22T07:00:00Z"), ZoneId.of("Asia/Seoul")));
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private static void reply(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @Test
    void returnsBodyVerbatimExceptOuterWhitespace() {
        // 안쪽 공백, 숫자 표기(1.50, 후행 0), 키 순서는 그대로 남아야 한다.
        String inner = "{\"dataHeader\": " + OK_HEADER + ", \"dataBody\": {\"b\": \"  10250\", \"a\": 1.50, \"c\": 1e3}}";
        apiBody = "\n  " + inner + "  \n";

        assertThat(client.marketSummaryRaw()).isEqualTo(inner);
    }

    @Test
    void dailyPriceSendsTheAgreedRequest() {
        apiBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{}}";

        client.dailyPriceRaw("TEST01", "0", 30);

        assertThat(requestBodies.get("/api/v1/ivs11560")).contains(
                "\"info_ccd\":\"1\"", "\"mkt_clsf\":\"0\"", "\"chrt_clsf\":\"D\"", "\"minute_tck_indx\":\"1\"",
                "\"is_cd\":\"TEST01\"", "\"inq_clsf\":\"2\"", "\"inq_cnt\":\"30\"");
        assertThat(authHeaders.get("/api/v1/ivs11560")).isEqualTo("Bearer tok-1");
    }

    @Test
    void worldIndexAndSummariesUseTheirOwnEndpoints() {
        apiBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{}}";

        client.worldIndexRaw("C");
        client.exchangeRateSummaryRaw();
        client.marketSummaryRaw();

        assertThat(requestBodies.get("/api/v1/iva60140")).contains("\"lnd_clsf\":\"C\"");
        assertThat(requestBodies.get("/api/v1/iva60190")).contains("\"dataBody\":{}");
        assertThat(requestBodies.get("/api/v1/ivsa0070")).contains("\"dataBody\":{}");
    }

    @Test
    void serverErrorKeepsProcessCodeButNotServerMessage() {
        apiStatus = 500;
        apiBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"E777\",\"processMessage\":\"테스트용 서버 오류 문구\"}}";

        assertThatThrownBy(() -> client.marketSummaryRaw())
                .isInstanceOf(BrokerApiException.class)
                .hasMessageContaining("http=500")
                .hasMessageContaining("processCode=E777")
                .hasMessageNotContaining("테스트용 서버 오류 문구");
    }

    @Test
    void failedResultCodeWithHttp200IsAnError() {
        apiBody = "{\"dataHeader\":{\"resultCode\":\"9000\",\"processCode\":\"bad code!\"}}";

        assertThatThrownBy(() -> client.marketSummaryRaw())
                .isInstanceOf(BrokerApiException.class)
                .hasMessageContaining("processCode=invalid")
                .hasMessageNotContaining("bad code");
    }

    @Test
    void nonJsonBodyFailsWithoutEchoingIt() {
        apiBody = "secret-looking-body <html>";

        assertThatThrownBy(() -> client.marketSummaryRaw())
                .isInstanceOf(BrokerApiException.class)
                .hasMessageNotContaining("secret-looking-body");
    }
}

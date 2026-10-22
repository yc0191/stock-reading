package dev.stockreading.collector.disclosure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

class DartClientTest {

    private static final LocalDate FROM = LocalDate.of(2026, 10, 1);
    private static final LocalDate TO = LocalDate.of(2026, 10, 20);

    private HttpServer server;
    private final List<String> queries = new ArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private DartClient clientReturning(int httpStatus, Function<String, String> bodyByQuery, String apiKey) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/list.json", exchange -> {
            String query = exchange.getRequestURI().getRawQuery();
            queries.add(query);
            byte[] body = bodyByQuery.apply(query).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(httpStatus, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        var properties = new DartProperties("http://127.0.0.1:" + server.getAddress().getPort() + "/api", apiKey, 100, 5);
        return new DartClient(properties, JsonMapper.builder().build());
    }

    private static String item(String receiptNo) {
        return """
                {"corp_code":"00000001","corp_name":"테스트기업","stock_code":"TEST01","corp_cls":"Y",
                 "report_nm":"분기보고서","rcept_no":"%s","flr_nm":"테스트기업","rcept_dt":"20261015","rm":""}
                """.formatted(receiptNo);
    }

    @Test
    void parsesListAndSendsDatesAndKey() throws IOException {
        var client = clientReturning(200, q -> """
                {"status":"000","message":"정상","page_no":1,"page_count":100,"total_count":1,"total_page":1,
                 "list":[%s]}""".formatted(item("20261015000001")), "k-1");

        List<Disclosure> result = client.list("00000001", FROM, TO);

        assertThat(result).hasSize(1);
        Disclosure d = result.get(0);
        assertThat(d.receiptNo()).isEqualTo("20261015000001");
        assertThat(d.stockCode()).isEqualTo("TEST01");
        assertThat(d.receivedDate()).isEqualTo(LocalDate.of(2026, 10, 15));
        assertThat(queries.get(0)).contains("crtfc_key=k-1", "bgn_de=20261001", "end_de=20261020", "corp_code=00000001");
    }

    @Test
    void noDataStatusIsEmptyNotError() throws IOException {
        var client = clientReturning(200, q -> "{\"status\":\"013\",\"message\":\"조회된 데이타가 없습니다.\"}", "k");

        assertThat(client.list(null, FROM, TO)).isEmpty();
        assertThat(queries.get(0)).doesNotContain("corp_code");
    }

    @Test
    void followsPagination() throws IOException {
        var client = clientReturning(200, q -> q.contains("page_no=1")
                ? "{\"status\":\"000\",\"total_page\":2,\"list\":[" + item("1") + "]}"
                : "{\"status\":\"000\",\"total_page\":2,\"list\":[" + item("2") + "]}", "k");

        assertThat(client.list(null, FROM, TO)).extracting(Disclosure::receiptNo).containsExactly("1", "2");
    }

    @Test
    void apiErrorStatusThrowsWithoutLeakingKey() throws IOException {
        var client = clientReturning(200, q -> "{\"status\":\"020\",\"message\":\"요청 제한을 초과하였습니다.\"}", "secret-key");

        assertThatThrownBy(() -> client.list(null, FROM, TO))
                .isInstanceOf(DartApiException.class)
                .hasMessageContaining("020")
                .hasMessageNotContaining("secret-key");
    }

    @Test
    void httpErrorThrowsWithoutLeakingKey() throws IOException {
        var client = clientReturning(500, q -> "boom", "secret-key");

        assertThatThrownBy(() -> client.list(null, FROM, TO))
                .isInstanceOf(DartApiException.class)
                .hasMessageContaining("500")
                .hasMessageNotContaining("secret-key");
    }

    @Test
    void missingKeyFailsBeforeAnyCall() throws IOException {
        var client = clientReturning(200, q -> "{}", "");

        assertThatThrownBy(() -> client.list(null, FROM, TO)).isInstanceOf(DartApiException.class);
        assertThat(queries).isEmpty();
    }

    @Test
    void propertiesToStringHidesKey() {
        assertThat(new DartProperties("u", "secret-key", 100, 5).toString()).doesNotContain("secret-key");
    }
}

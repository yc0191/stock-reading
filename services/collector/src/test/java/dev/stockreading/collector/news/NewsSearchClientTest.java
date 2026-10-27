package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import tools.jackson.databind.json.JsonMapper;

class NewsSearchClientTest {

    private HttpServer server;
    private final List<String> queries = new ArrayList<>();
    private final List<String> idHeaders = new ArrayList<>();
    private final List<String> secretHeaders = new ArrayList<>();

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private NewsSearchClient client(int status, String body, String id, String secret) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/search/news.json", exchange -> {
            queries.add(exchange.getRequestURI().getRawQuery());
            idHeaders.add(exchange.getRequestHeaders().getFirst("X-Test-Id"));
            secretHeaders.add(exchange.getRequestHeaders().getFirst("X-Test-Secret"));
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        var properties = new NewsSearchProperties("http://127.0.0.1:" + server.getAddress().getPort(), id, secret,
                "X-Test-Id", "X-Test-Secret", 20, 5, "", 100, 0);
        return new NewsSearchClient(properties, JsonMapper.builder().build());
    }

    private static final String RESPONSE = """
            {"lastBuildDate":"Thu, 22 Oct 2026 16:10:09 +0900","total":2,"start":1,"display":2,"items":[
              {"title":"<b>테스트종목</b>, &quot;AI&quot; 투자 확대","originallink":"https://www.news.example.com/a?id=1",
               "link":"https://news.example.com/m/1","description":"요약 <b>테스트종목</b> &amp; 반도체",
               "pubDate":"Thu, 22 Oct 2026 16:08:00 +0900"},
              {"title":"링크 없는 기사","originallink":"","link":"","description":"x","pubDate":"Thu, 22 Oct 2026 16:07:00 +0900"},
              {"title":"시각 이상한 기사","originallink":"https://x.example.com/b","link":"","description":"x","pubDate":"어제"}
            ]}""";

    @Test
    void cleansFieldsParsesTimeAndDropsItemsMissingLinkOrTime() throws IOException {
        var client = client(200, RESPONSE, "id", "secret");

        List<NewsItem> items = client.search("테스트검색어", 20);

        assertThat(items).hasSize(1);
        NewsItem item = items.get(0);
        assertThat(item.title()).isEqualTo("테스트종목, \"AI\" 투자 확대");
        assertThat(item.summary()).isEqualTo("요약 테스트종목 & 반도체");
        assertThat(item.url()).isEqualTo("https://www.news.example.com/a?id=1");
        assertThat(item.providerUrl()).isEqualTo("https://news.example.com/m/1");
        assertThat(item.source()).isEqualTo("news.example.com");
        assertThat(item.publishedAt().toString()).isEqualTo("2026-10-22T16:08+09:00");
        assertThat(item.matchedQueries()).containsExactly("테스트검색어");
    }

    @Test
    void sendsEncodedQueryDisplaySortAndAuthHeaders() throws IOException {
        var client = client(200, "{\"items\":[]}", "my-id", "my-secret");

        client.search("테스트 시황", 50);

        String raw = queries.get(0);
        assertThat(URLDecoder.decode(raw, StandardCharsets.UTF_8)).contains("query=테스트 시황", "display=50", "sort=date", "start=1");
        assertThat(idHeaders.get(0)).isEqualTo("my-id");
        assertThat(secretHeaders.get(0)).isEqualTo("my-secret");
    }

    @Test
    void errorResponseThrowsWithCodeButNoSecrets() throws IOException {
        var client = client(403, "{\"errorMessage\":\"Scope Status Invalid\",\"errorCode\":\"403\"}", "secret-id", "secret-secret");

        assertThatThrownBy(() -> client.search("테스트검색어", 10))
                .isInstanceOf(NewsSearchApiException.class)
                .hasMessageContaining("403")
                .hasMessageNotContaining("secret-id")
                .hasMessageNotContaining("secret-secret");
    }

    @Test
    void nonJsonErrorBodyStillThrowsApiException() throws IOException {
        var client = client(500, "<html>boom</html>", "id", "secret");

        assertThatThrownBy(() -> client.search("테스트검색어", 10)).isInstanceOf(NewsSearchApiException.class).hasMessageContaining("500");
    }

    @Test
    void missingCredentialsFailBeforeAnyCall() throws IOException {
        var client = client(200, "{}", "", "");

        assertThatThrownBy(() -> client.search("테스트검색어", 10)).isInstanceOf(NewsSearchApiException.class);
        assertThat(queries).isEmpty();
    }

    @Test
    void missingHeaderNamesFailBeforeAnyCall() throws IOException {
        var properties = new NewsSearchProperties("http://127.0.0.1:1", "id", "secret", "", "", 20, 5, "", 100, 0);
        var client = new NewsSearchClient(properties, JsonMapper.builder().build());

        assertThatThrownBy(() -> client.search("테스트검색어", 10)).isInstanceOf(NewsSearchApiException.class);
    }

    @Test
    void rejectsOutOfRangeDisplay() throws IOException {
        var client = client(200, "{}", "id", "secret");

        assertThatThrownBy(() -> client.search("테스트검색어", 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> client.search("테스트검색어", 101)).isInstanceOf(IllegalArgumentException.class);
        assertThat(queries).isEmpty();
    }

    @Test
    void propertiesToStringHidesSecrets() {
        assertThat(new NewsSearchProperties("u", "secret-id", "secret-secret", "h1", "h2", 20, 5, "q=A", 100, 0).toString())
                .doesNotContain("secret-id", "secret-secret");
    }
}

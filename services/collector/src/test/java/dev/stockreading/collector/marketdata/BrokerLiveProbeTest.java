package dev.stockreading.collector.marketdata;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

// 시세 제공자의 운영 서버에 토큰 1회와 종목별 현재가를 호출해 응답 형식을 확인하는 수동 도구다.
// 평소 테스트 실행에서는 건너뛰고, BROKER_PROBE=1 일 때만 동작한다. 종목은 BROKER_PROBE_SYMBOLS 로 받고 코드에 두지 않는다. 요약은 BROKER_PROBE_OUT 에, 응답 원문은 BROKER_PROBE_RAW_DIR 에 쓴다.
// 실제 운영 서버를 호출하므로 조회 API 두 개 외에는 호출하지 않는다.
// 토큰은 한 번 발급해 모든 호출에 재사용하고, 키와 토큰 값은 파일과 출력 어디에도 남기지 않는다.
@EnabledIfEnvironmentVariable(named = "BROKER_PROBE", matches = "1")
class BrokerLiveProbeTest {

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final List<String> report = new ArrayList<>();
    private Path rawDir;

    @Test
    void probe() throws Exception {
        String baseUrl = System.getenv("BROKER_BASE_URL");
        String appKey = System.getenv("BROKER_APP_KEY");
        String appSecret = System.getenv("BROKER_APP_SECRET");
        String[] symbols = System.getenv("BROKER_PROBE_SYMBOLS").split(",");
        Path out = Path.of(System.getenv("BROKER_PROBE_OUT"));
        rawDir = Path.of(System.getenv("BROKER_PROBE_RAW_DIR"));
        Files.createDirectories(rawDir);
        try {
            report.add("baseUrl=" + baseUrl + " keyLength=" + appKey.length() + " secretLength=" + appSecret.length());

            ObjectNode tokenBody = mapper.createObjectNode();
            tokenBody.put("appKey", appKey);
            tokenBody.put("appSecret", appSecret);
            tokenBody.put("grantType", "client_credentials");
            JsonNode token = call(baseUrl + "/oauth2/token", tokenBody, null, "token");
            String accessToken = token.path("dataBody").path("access_token").asString("");
            report.add("token: access_token 길이=" + accessToken.length()
                    + " token_type=" + token.path("dataBody").path("token_type").asString("")
                    + " expires_in=" + token.path("dataBody").path("expires_in"));
            if (accessToken.isBlank()) {
                return;
            }

            for (String symbol : symbols) {
                ObjectNode priceBody = mapper.createObjectNode();
                priceBody.put("excg_clsf", "0");
                priceBody.put("shrt_cd", symbol.trim());
                call(baseUrl + "/api/v1/ivu10140", priceBody, accessToken, "price-" + symbol.trim());
            }
        } finally {
            Files.writeString(out, String.join("\n", report) + "\n", StandardCharsets.UTF_8);
        }
    }

    // 응답은 헤더(결과 코드, 처리 시각)만 기록하고, 본문은 호출자가 필요한 부분만 골라 기록한다.
    private JsonNode call(String url, ObjectNode dataBody, String token, String label) throws Exception {
        ObjectNode request = mapper.createObjectNode();
        ObjectNode header = request.putObject("dataHeader");
        header.put("ipAddr", "127.0.0.1");
        header.put("macAddr", "00:00:00:00:00:00");
        request.set("dataBody", dataBody);

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request), StandardCharsets.UTF_8));
        if (token != null) {
            builder.header("Authorization", "Bearer " + token);
        }
        long start = System.nanoTime();
        HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        long millis = (System.nanoTime() - start) / 1_000_000;

        // 응답 원문은 가공 없이 그대로 저장한다. 토큰 응답만 access_token 값을 가린다.
        String raw = "token".equals(label)
                ? response.body().replaceAll("(\"access_token\"\\s*:\\s*\")[^\"]*", "$1<REDACTED>")
                : response.body();
        Files.writeString(rawDir.resolve(label + ".json"), raw, StandardCharsets.UTF_8);
        report.add(label + ": http=" + response.statusCode() + " " + millis + "ms"
                + " contentType=" + response.headers().firstValue("Content-Type").orElse("-"));
        JsonNode json;
        try {
            json = mapper.readTree(response.body());
        } catch (RuntimeException e) {
            // 토큰이 섞이지 않도록 JSON 이 아닌 응답은 앞부분만 남긴다.
            report.add(label + ": JSON 아님, 본문 앞 200자=" + response.body().substring(0, Math.min(200, response.body().length())));
            throw e;
        }
        report.add(label + ": dataHeader=" + mapper.writeValueAsString(json.path("dataHeader")));
        return json;
    }
}

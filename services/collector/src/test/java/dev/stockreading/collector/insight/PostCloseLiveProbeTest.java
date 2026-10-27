package dev.stockreading.collector.insight;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

// 장 마감 후 인사이트용 조회 API(일봉, 세계지수, 환율종합, 시장종합)의 응답 형식을 확인하는 수동 도구다.
// 평소 테스트 실행에서는 건너뛰고, BROKER_POSTCLOSE_PROBE=1 일 때만 운영 서버를 호출한다.
// 토큰 1회와 API당 1회씩만 호출한다. 종목은 BROKER_PROBE_SYMBOLS 의 첫 번째만 쓰고 코드에 두지 않는다.
// 요약은 BROKER_PROBE_OUT 에, 응답 원문은 BROKER_PROBE_RAW_DIR 에 쓴다(둘 다 gitignore 대상 경로).
// 조회 건수(BROKER_POSTCLOSE_PROBE_COUNT, 기본 30), 시장구분(BROKER_POSTCLOSE_PROBE_MARKET, 기본 0),
// 세계지수 대륙구분(BROKER_POSTCLOSE_PROBE_CONTINENT, 기본 C)은 쉼표로 여러 개를 줄 수 있고 대륙마다 한 번씩 호출한다.
// 호출할 API는 BROKER_POSTCLOSE_PROBE_APIS(daily, world, exchange, overview 를 쉼표로, 기본 전부)로 고른다.
// price 를 고르면 현재가를 거래소 구분(BROKER_POSTCLOSE_PROBE_EXCHANGES, 기본 0,1,2)마다 한 번씩 호출한다.
// 일봉의 거래량이 어느 거래소 기준인지 현재가의 거래소별 누적 거래량과 맞춰 보려는 용도다.
// 키와 토큰 값은 파일과 출력 어디에도 남기지 않는다.
@EnabledIfEnvironmentVariable(named = "BROKER_POSTCLOSE_PROBE", matches = "1")
class PostCloseLiveProbeTest {

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final List<String> report = new ArrayList<>();
    private Path rawDir;

    @Test
    void probe() throws Exception {
        String baseUrl = System.getenv("BROKER_BASE_URL");
        String appKey = System.getenv("BROKER_APP_KEY");
        String appSecret = System.getenv("BROKER_APP_SECRET");
        String symbol = System.getenv("BROKER_PROBE_SYMBOLS").split(",")[0].trim();
        String count = System.getenv().getOrDefault("BROKER_POSTCLOSE_PROBE_COUNT", "30");
        String market = System.getenv().getOrDefault("BROKER_POSTCLOSE_PROBE_MARKET", "0");
        String[] continents = System.getenv().getOrDefault("BROKER_POSTCLOSE_PROBE_CONTINENT", "C").split(",");
        String[] exchanges = System.getenv().getOrDefault("BROKER_POSTCLOSE_PROBE_EXCHANGES", "0,1,2").split(",");
        Set<String> apis = Arrays.stream(System.getenv()
                        .getOrDefault("BROKER_POSTCLOSE_PROBE_APIS", "daily,world,exchange,overview").split(","))
                .map(String::trim).collect(Collectors.toSet());
        Path out = Path.of(System.getenv("BROKER_PROBE_OUT"));
        rawDir = Path.of(System.getenv("BROKER_PROBE_RAW_DIR"));
        Files.createDirectories(rawDir);
        try {
            report.add("keyLength=" + appKey.length() + " secretLength=" + appSecret.length()
                    + " count=" + count + " market=" + market + " continents=" + String.join(",", continents)
                    + " apis=" + String.join(",", new java.util.TreeSet<>(apis)));

            ObjectNode tokenBody = mapper.createObjectNode();
            tokenBody.put("appKey", appKey);
            tokenBody.put("appSecret", appSecret);
            tokenBody.put("grantType", "client_credentials");
            JsonNode token = call(baseUrl + "/oauth2/token", tokenBody, null, "token");
            String accessToken = token.path("dataBody").path("access_token").asString("");
            report.add("token: access_token 길이=" + accessToken.length()
                    + " expires_in=" + token.path("dataBody").path("expires_in"));
            if (accessToken.isBlank()) {
                return;
            }

            if (apis.contains("daily")) {
                // minute_tck_indx 는 선택 항목이지만 일봉의 조회 주기를 1로 명시해 서버 기본값에 기대지 않는다.
                ObjectNode chartBody = mapper.createObjectNode();
                chartBody.put("info_ccd", "1");
                chartBody.put("mkt_clsf", market);
                chartBody.put("chrt_clsf", "D");
                chartBody.put("minute_tck_indx", "1");
                chartBody.put("is_cd", symbol);
                chartBody.put("inq_clsf", "2");
                chartBody.put("inq_cnt", count);
                JsonNode chart = call(baseUrl + "/api/v1/ivs11560", chartBody, accessToken, "daily-chart");
                summarizeRecords(chart.path("dataBody").path("out2"), "daily-chart", "dt");
            }

            if (apis.contains("price")) {
                for (String exchange : exchanges) {
                    String label = "price-" + exchange.trim();
                    ObjectNode priceBody = mapper.createObjectNode();
                    priceBody.put("excg_clsf", exchange.trim());
                    priceBody.put("shrt_cd", symbol);
                    call(baseUrl + "/api/v1/ivu10140", priceBody, accessToken, label);
                }
            }

            if (apis.contains("world")) {
                for (String continent : continents) {
                    String label = "world-index-" + continent.trim();
                    ObjectNode worldBody = mapper.createObjectNode();
                    worldBody.put("lnd_clsf", continent.trim());
                    JsonNode world = call(baseUrl + "/api/v1/iva60140", worldBody, accessToken, label);
                    summarizeRecords(world.path("dataBody").path("out2"), label, "is_cd");
                }
            }

            if (apis.contains("exchange")) {
                JsonNode exchange = call(baseUrl + "/api/v1/iva60190", mapper.createObjectNode(), accessToken, "exchange-rate");
                summarizeRecords(exchange.path("dataBody").path("out2"), "exchange-rate", "crncy_cd");
            }

            if (apis.contains("overview")) {
                JsonNode overview = call(baseUrl + "/api/v1/ivsa0070", mapper.createObjectNode(), accessToken, "market-overview");
                summarizeRecords(overview.path("dataBody").path("out2"), "market-overview 지수", "indx_id");
                summarizeRecords(overview.path("dataBody").path("out3"), "market-overview 종목", "is_cd");
            }
        } finally {
            Files.writeString(out, String.join("\n", report) + "\n", StandardCharsets.UTF_8);
        }
    }

    // 값 자체는 원문 파일에 있으므로 요약에는 건수와 키 필드의 처음·끝만 남긴다.
    private void summarizeRecords(JsonNode records, String label, String keyField) {
        int size = records.size();
        if (size == 0) {
            report.add(label + ": 레코드 없음");
            return;
        }
        report.add(label + ": 레코드 " + size + "건 첫 " + keyField + "=" + records.get(0).path(keyField).asString("")
                + " 끝 " + keyField + "=" + records.get(size - 1).path(keyField).asString(""));
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
                + " bytes=" + response.body().length());
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

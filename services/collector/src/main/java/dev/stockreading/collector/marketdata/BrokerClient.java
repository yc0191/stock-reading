package dev.stockreading.collector.marketdata;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

// 시세 조회와 토큰 발급만 한다. 같은 키로 주문·계좌 API도 호출할 수 있지만, 읽기 전용 원칙(ADR-007)과
// 개인 금액·수량을 이벤트에 담지 않는 규칙 때문에 일부러 만들지 않는다.
@Component
class BrokerClient {

    static final String RESULT_OK = "200";
    private static final String HEADER_IP = "127.0.0.1";
    private static final String HEADER_MAC = "00:00:00:00:00:00";
    private static final DateTimeFormatter PROCESS_TIME = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    // 만료 직전에 보낸 요청이 실패하지 않도록 이 시간만큼 일찍 재발급한다.
    private static final Duration TOKEN_MARGIN = Duration.ofMinutes(5);

    private final BrokerProperties properties;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final HttpClient http;

    private String accessToken;
    private Instant tokenExpiresAt = Instant.MIN;

    BrokerClient(BrokerProperties properties, ObjectMapper mapper, Clock clock) {
        this.properties = properties;
        this.mapper = mapper;
        this.clock = clock;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(properties.timeoutSeconds())).build();
    }

    // 응답 본문에 시세 시각이 없어서 응답 헤더의 처리 시각을 asOf 로 쓴다.
    PriceQuote currentPrice(String symbol) {
        ObjectNode body = mapper.createObjectNode();
        body.put("excg_clsf", "0");
        body.put("shrt_cd", symbol);
        JsonNode response = post("/api/v1/ivu10140", body, true);

        JsonNode data = response.path("dataBody");
        long price = parsePrice(data.path("now_prc").asString(""), symbol, response.path("dataHeader"));
        return new PriceQuote(symbol, price, processedAt(response.path("dataHeader")));
    }

    private synchronized String token() {
        if (accessToken != null && clock.instant().isBefore(tokenExpiresAt)) {
            return accessToken;
        }
        ObjectNode body = mapper.createObjectNode();
        body.put("appKey", properties.appKey());
        body.put("appSecret", properties.appSecret());
        body.put("grantType", "client_credentials");
        JsonNode data = post("/oauth2/token", body, false).path("dataBody");

        String token = data.path("access_token").asString("");
        long expiresIn = data.path("expires_in").asLong(0);
        if (token.isBlank() || expiresIn <= 0) {
            throw new BrokerApiException("토큰 응답이 올바르지 않습니다");
        }
        accessToken = token;
        tokenExpiresAt = clock.instant().plusSeconds(expiresIn).minus(TOKEN_MARGIN);
        return accessToken;
    }

    private JsonNode post(String path, ObjectNode dataBody, boolean authorized) {
        if (isBlank(properties.baseUrl()) || isBlank(properties.appKey()) || isBlank(properties.appSecret())) {
            throw new BrokerApiException("BROKER_BASE_URL / BROKER_APP_KEY / BROKER_APP_SECRET 이 설정되지 않았습니다");
        }
        ObjectNode request = mapper.createObjectNode();
        ObjectNode header = request.putObject("dataHeader");
        header.put("ipAddr", HEADER_IP);
        header.put("macAddr", HEADER_MAC);
        request.set("dataBody", dataBody);

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(properties.baseUrl() + path))
                .timeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .header("Content-Type", "application/json");
        if (authorized) {
            builder.header("Authorization", "Bearer " + token());
        }
        HttpRequest httpRequest = builder.POST(HttpRequest.BodyPublishers.ofString(
                mapper.writeValueAsString(request), StandardCharsets.UTF_8)).build();
        try {
            HttpResponse<String> response = http.send(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            JsonNode json = mapper.readTree(response.body());
            JsonNode responseHeader = json.path("dataHeader");
            if (response.statusCode() != 200 || !RESULT_OK.equals(responseHeader.path("resultCode").asString(""))) {
                // 서버 문구는 외부 원문이라 남기지 않고 원인 구분용 코드만 남긴다.
                throw new BrokerApiException("시세 API 오류 http=" + response.statusCode()
                        + " processCode=" + safeCode(responseHeader.path("processCode").asString("")));
            }
            return json;
        } catch (IOException e) {
            // 원인 예외 메시지에 요청 정보가 들어갈 수 있어 연결하지 않는다.
            throw new BrokerApiException("시세 API 호출 실패: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BrokerApiException("시세 API 호출 중단", e);
        }
    }

    private static long parsePrice(String raw, String symbol, JsonNode header) {
        try {
            long price = Long.parseLong(raw.trim());
            if (price <= 0) {
                throw new BrokerApiException("현재가가 0 이하입니다 symbol=" + symbol);
            }
            return price;
        } catch (NumberFormatException e) {
            // 없는 종목 등은 HTTP 200, resultCode 200 으로 오고 본문만 비어 있다. 서버 문구는 외부 원문이라 남기지 않고 원인 구분용 코드만 남긴다.
            throw new BrokerApiException("현재가를 읽을 수 없습니다 symbol=" + symbol
                    + " processCode=" + safeCode(header.path("processCode").asString("")));
        }
    }

    // 외부 응답의 코드는 짧은 영숫자일 때만 로그에 남긴다.
    private static String safeCode(String code) {
        return code.matches("[A-Za-z0-9]{1,8}") ? code : "invalid";
    }

    private OffsetDateTime processedAt(JsonNode header) {
        String raw = header.path("processTime").asString("");
        try {
            return LocalDateTime.parse(raw, PROCESS_TIME).atZone(clock.getZone()).toOffsetDateTime()
                    .truncatedTo(ChronoUnit.SECONDS);
        } catch (RuntimeException e) {
            return OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    // 아래는 마감 후 조회용이다. 응답을 파싱한 결과가 아니라 본문 원문(trim 만)을 돌려준다.
    // 재직렬화하면 숫자 표기와 공백이 바뀌어 원문 보존 원칙을 어기기 때문이다.
    String dailyPriceRaw(String symbol, String marketClass, int count) {
        ObjectNode body = mapper.createObjectNode();
        body.put("info_ccd", "1");
        body.put("mkt_clsf", marketClass);
        body.put("chrt_clsf", "D");
        body.put("minute_tck_indx", "1");
        body.put("is_cd", symbol);
        body.put("inq_clsf", "2");
        body.put("inq_cnt", String.valueOf(count));
        return postRaw("/api/v1/ivs11560", body);
    }

    String worldIndexRaw(String continent) {
        ObjectNode body = mapper.createObjectNode();
        body.put("lnd_clsf", continent);
        return postRaw("/api/v1/iva60140", body);
    }

    String exchangeRateSummaryRaw() {
        return postRaw("/api/v1/iva60190", mapper.createObjectNode());
    }

    String marketSummaryRaw() {
        return postRaw("/api/v1/ivsa0070", mapper.createObjectNode());
    }

    // post() 와 같은 요청 형식이며, 성공 여부만 확인하고 응답 본문은 그대로 돌려준다.
    private String postRaw(String path, ObjectNode dataBody) {
        if (isBlank(properties.baseUrl()) || isBlank(properties.appKey()) || isBlank(properties.appSecret())) {
            throw new BrokerApiException("BROKER_BASE_URL / BROKER_APP_KEY / BROKER_APP_SECRET 이 설정되지 않았습니다");
        }
        ObjectNode request = mapper.createObjectNode();
        ObjectNode header = request.putObject("dataHeader");
        header.put("ipAddr", HEADER_IP);
        header.put("macAddr", HEADER_MAC);
        request.set("dataBody", dataBody);

        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(properties.baseUrl() + path))
                .timeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + token())
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request), StandardCharsets.UTF_8))
                .build();
        try {
            HttpResponse<String> response = http.send(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            JsonNode responseHeader = mapper.readTree(response.body()).path("dataHeader");
            if (response.statusCode() != 200 || !RESULT_OK.equals(responseHeader.path("resultCode").asString(""))) {
                // 서버 문구는 외부 원문이라 남기지 않고 원인 구분용 코드만 남긴다.
                throw new BrokerApiException("시세 API 오류 http=" + response.statusCode()
                        + " processCode=" + safeCode(responseHeader.path("processCode").asString("")));
            }
            return response.body().trim();
        } catch (tools.jackson.core.JacksonException e) {
            // 파싱 예외 메시지에 응답 일부가 들어갈 수 있어 종류만 남긴다.
            throw new BrokerApiException("시세 API 응답 형식 오류");
        } catch (IOException e) {
            // 원인 예외 메시지에 요청 정보가 들어갈 수 있어 연결하지 않는다.
            throw new BrokerApiException("시세 API 호출 실패: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BrokerApiException("시세 API 호출 중단", e);
        }
    }
}

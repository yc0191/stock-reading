package dev.stockreading.collector.marketdata;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

// 시세 조회와 토큰 발급만 한다. 같은 키로 주문·계좌 API도 호출할 수 있지만, 읽기 전용 원칙(ADR-007)과
// 개인 금액·수량을 이벤트에 담지 않는 규칙 때문에 일부러 만들지 않는다.
@Component
class BrokerClient {

    static final String RESULT_OK = "200";
    private static final int HTTP_UNAUTHORIZED = 401;
    // 재시도 호출도 호출하는 쪽과 같은 상한 장치를 거친다. 호출 스레드를 오래 잡지 않도록 짧게만 기다린다.
    private static final Duration RETRY_LIMITER_WAIT = Duration.ofSeconds(1);
    private static final Logger log = LoggerFactory.getLogger(BrokerClient.class);
    private static final String HEADER_IP = "127.0.0.1";
    private static final String HEADER_MAC = "00:00:00:00:00:00";
    private static final DateTimeFormatter PROCESS_TIME = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    // 만료 직전에 보낸 요청이 실패하지 않도록 이 시간만큼 일찍 재발급한다.
    private static final Duration TOKEN_MARGIN = Duration.ofMinutes(5);
    // 발급이 실패한 직후에 락을 기다리던 호출들이 같은 요청을 줄줄이 다시 보내지 않도록 이 시간 동안은 직전 실패를 그대로 돌려준다.
    private static final Duration TOKEN_FAILURE_COOLDOWN = Duration.ofMillis(500);
    private static final String MASK = "***";

    /** 토큰은 메모리에만 두고 이 값 하나로 교체한다. 읽는 쪽은 락 없이 읽는다. */
    private record Token(String value, Instant issuedAt, Instant refreshAt, Instant expiresAt) {
    }

    private record TokenFailure(Instant at, BrokerApiException error) {
    }

    private final BrokerProperties properties;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final HttpClient http;
    private final CallRateLimit limiter;
    private final ReentrantLock issueLock = new ReentrantLock();

    private volatile Token token;
    private volatile TokenFailure lastTokenFailure;

    BrokerClient(BrokerProperties properties, ObjectMapper mapper, Clock clock, CallRateLimit limiter) {
        this.properties = properties;
        this.limiter = limiter;
        this.mapper = mapper;
        this.clock = clock;
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(properties.timeoutSeconds())).build();
    }

    // 응답 본문에 시세 시각이 없어서 응답 헤더의 처리 시각을 asOf 로 쓴다.
    PriceQuote currentPrice(String symbol) {
        ObjectNode body = mapper.createObjectNode();
        body.put("excg_clsf", "0");
        body.put("shrt_cd", symbol);
        Reply reply = post("/api/v1/ivu10140", body, true);

        JsonNode data = reply.json().path("dataBody");
        JsonNode header = reply.json().path("dataHeader");
        long price = parsePrice(data.path("now_prc").asString(""), symbol, header, reply);
        // 원문은 trim 외에 가공하지 않는다. 토큰 값만 예외로 가린다.
        return new PriceQuote(symbol, price, processedAt(header), redact(reply.raw(), false));
    }

    /** 실패 시점의 토큰 상태. 토큰 값은 돌려주지 않고, 아직 발급한 적이 없으면 null 이다. */
    record TokenState(long ageMs, long remainingMs) {
    }

    TokenState tokenState() {
        Token current = token;
        if (current == null) {
            return null;
        }
        Instant now = clock.instant();
        return new TokenState(Duration.between(current.issuedAt(), now).toMillis(),
                Duration.between(now, current.expiresAt()).toMillis());
    }

    private String token() {
        Token current = token;
        if (isFresh(current)) {
            return current.value();
        }
        issueLock.lock();
        try {
            current = token;
            if (isFresh(current)) {
                return current.value();
            }
            TokenFailure failure = lastTokenFailure;
            if (failure != null && clock.instant().isBefore(failure.at().plus(TOKEN_FAILURE_COOLDOWN))) {
                throw failure.error();
            }
            try {
                current = issueToken();
            } catch (BrokerApiException e) {
                BrokerApiException asTokenFailure = new BrokerApiException(FailureType.TOKEN_ISSUE_FAILED, e.getMessage(),
                        e.httpStatus(), e.responseCode(), e.exceptionType(), e.responseRaw());
                lastTokenFailure = new TokenFailure(clock.instant(), asTokenFailure);
                throw asTokenFailure;
            }
            lastTokenFailure = null;
            token = current;
            return current.value();
        } finally {
            issueLock.unlock();
        }
    }

    private boolean isFresh(Token current) {
        return current != null && clock.instant().isBefore(current.refreshAt());
    }

    private Token issueToken() {
        ObjectNode body = mapper.createObjectNode();
        body.put("appKey", properties.appKey());
        body.put("appSecret", properties.appSecret());
        body.put("grantType", "client_credentials");
        JsonNode data = post("/oauth2/token", body, false).json().path("dataBody");

        String value = data.path("access_token").asString("");
        long expiresIn = data.path("expires_in").asLong(0);
        if (value.isBlank() || expiresIn <= 0) {
            // 토큰 응답 원문은 어디에도 싣지 않는다.
            throw new BrokerApiException(FailureType.TOKEN_ISSUE_FAILED, "토큰 응답이 올바르지 않습니다");
        }
        Instant now = clock.instant();
        Instant expiresAt = now.plusSeconds(expiresIn);
        return new Token(value, now, expiresAt.minus(TOKEN_MARGIN), expiresAt);
    }

    private record Reply(JsonNode json, String raw, int status) {
    }

    private Reply post(String path, ObjectNode dataBody, boolean authorized) {
        if (authorized) {
            return withTokenRetry(bearer -> send(path, dataBody, bearer));
        }
        return send(path, dataBody, null);
    }

    // 토큰이 거부되면(HTTP 401) 메모리의 토큰을 버리고 새로 발급해 한 번만 다시 시도한다.
    // 재시도도 거부되면 그 실패를 그대로 올리므로 다음 호출은 처음부터 새 토큰으로 시작한다.
    private <T> T withTokenRetry(Function<String, T> call) {
        String used = token();
        try {
            return call.apply(used);
        } catch (BrokerApiException e) {
            if (e.httpStatus() == null || e.httpStatus() != HTTP_UNAUTHORIZED) {
                throw e;
            }
            discardToken(used);
            if (!limiter.tryAcquire(RETRY_LIMITER_WAIT)) {
                log.warn("broker token rejected http={}; discarded it but no call slot for the retry, giving up",
                        HTTP_UNAUTHORIZED);
                throw e;
            }
            log.warn("broker token rejected http={}; discarded it and reissuing once", HTTP_UNAUTHORIZED);
            try {
                T result = call.apply(token());
                log.warn("broker call succeeded after token reissue");
                return result;
            } catch (BrokerApiException retryFailure) {
                log.warn("broker call failed again after token reissue type={} http={}",
                        retryFailure.failureType(), retryFailure.httpStatus());
                throw retryFailure;
            }
        }
    }

    // 동시에 거부된 호출들이 이미 새로 발급된 토큰까지 버리지 않도록, 거부된 그 토큰일 때만 버린다.
    private void discardToken(String rejected) {
        issueLock.lock();
        try {
            Token current = token;
            if (current != null && current.value().equals(rejected)) {
                token = null;
                lastTokenFailure = null;
            }
        } finally {
            issueLock.unlock();
        }
    }

    private Reply send(String path, ObjectNode dataBody, String bearer) {
        if (isBlank(properties.baseUrl()) || isBlank(properties.appKey()) || isBlank(properties.appSecret())) {
            throw new BrokerApiException(FailureType.TOKEN_ISSUE_FAILED,
                    "BROKER_BASE_URL / BROKER_APP_KEY / BROKER_APP_SECRET 이 설정되지 않았습니다");
        }
        ObjectNode request = mapper.createObjectNode();
        ObjectNode header = request.putObject("dataHeader");
        header.put("ipAddr", HEADER_IP);
        header.put("macAddr", HEADER_MAC);
        request.set("dataBody", dataBody);

        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(properties.baseUrl() + path))
                .timeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .header("Content-Type", "application/json");
        if (bearer != null) {
            builder.header("Authorization", "Bearer " + bearer);
        }
        HttpRequest httpRequest = builder.POST(HttpRequest.BodyPublishers.ofString(
                mapper.writeValueAsString(request), StandardCharsets.UTF_8)).build();
        HttpResponse<String> response;
        try {
            response = http.send(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            // 원인 예외 메시지에 요청 정보가 들어갈 수 있어 연결하지 않고 종류만 남긴다.
            FailureType type = e instanceof HttpTimeoutException ? FailureType.TIMEOUT : FailureType.CONNECTION_FAILED;
            throw new BrokerApiException(type, "시세 API 호출 실패: " + e.getClass().getSimpleName(),
                    null, null, e.getClass().getSimpleName(), null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 중단",
                    null, null, e.getClass().getSimpleName(), null);
        }

        int status = response.statusCode();
        String raw = response.body() == null ? "" : response.body().trim();
        JsonNode json;
        try {
            json = mapper.readTree(raw);
        } catch (RuntimeException e) {
            throw new BrokerApiException(status != 200 ? FailureType.HTTP_ERROR : FailureType.RESULT_CODE_FAILED,
                    "시세 API 응답을 해석할 수 없습니다 http=" + status, status, null,
                    e.getClass().getSimpleName(), redact(raw, true));
        }
        JsonNode responseHeader = json.path("dataHeader");
        if (status != 200 || !RESULT_OK.equals(responseHeader.path("resultCode").asString(""))) {
            // 서버 문구는 외부 원문이라 메시지와 로그에 남기지 않고 원인 구분용 코드만 남긴다.
            String code = safeCode(responseHeader.path("processCode").asString(""));
            throw new BrokerApiException(status != 200 ? FailureType.HTTP_ERROR : FailureType.RESULT_CODE_FAILED,
                    "시세 API 오류 http=" + status + " processCode=" + code,
                    status, code.isEmpty() ? null : code, null, redact(raw, true));
        }
        return new Reply(json, raw, status);
    }

    private long parsePrice(String raw, String symbol, JsonNode header, Reply reply) {
        String code = safeCode(header.path("processCode").asString(""));
        String responseCode = code.isEmpty() ? null : code;
        if (raw.isBlank()) {
            // 없는 종목 등은 HTTP 200, resultCode 200 으로 오고 본문만 비어 있다.
            throw new BrokerApiException(FailureType.EMPTY_PRICE,
                    "현재가를 읽을 수 없습니다 symbol=" + symbol + " processCode=" + code,
                    reply.status(), responseCode, null, redact(reply.raw(), true));
        }
        long price;
        try {
            price = Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            throw new BrokerApiException(FailureType.PRICE_PARSE_FAILED,
                    "현재가를 읽을 수 없습니다 symbol=" + symbol + " processCode=" + code,
                    reply.status(), responseCode, e.getClass().getSimpleName(), redact(reply.raw(), true));
        }
        if (price <= 0) {
            throw new BrokerApiException(FailureType.PRICE_PARSE_FAILED, "현재가가 0 이하입니다 symbol=" + symbol,
                    reply.status(), responseCode, null, redact(reply.raw(), true));
        }
        return price;
    }

    // 외부 응답의 코드는 짧은 영숫자일 때만 남기고, 비어 있으면 빈 문자열이다.
    private static String safeCode(String code) {
        if (code.isEmpty()) {
            return "";
        }
        return code.matches("[A-Za-z0-9]{1,8}") ? code : "invalid";
    }

    // 접근토큰은 항상, 키와 시크릿은 실패 원문에서만 가린다(오류 페이지가 요청을 되돌려 줄 수 있다).
    private String redact(String raw, boolean includeAppCredentials) {
        String result = raw;
        Token current = token;
        if (current != null) {
            result = mask(result, current.value());
        }
        if (includeAppCredentials) {
            result = mask(result, properties.appKey());
            result = mask(result, properties.appSecret());
        }
        return result;
    }

    private static String mask(String text, String secret) {
        return isBlank(secret) ? text : text.replace(secret, MASK);
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

    // 해외주식 통합차트의 일봉. 거래소 코드(NAS, NYS, AMX)와 티커로 조회한다.
    String overseasDailyPriceRaw(String exchangeCode, String ticker, int count) {
        ObjectNode body = mapper.createObjectNode();
        body.put("krx_cd", exchangeCode);
        body.put("is_cd", ticker);
        body.put("chrt_clsf", "3");
        body.put("rcrd_c", String.valueOf(count));
        return postRaw("/api/v1/gsc10060", body);
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

    // 장 운영 상태. 휴장일 판정에 쓴다. 경로는 설정으로 받고(공개 저장소에 제공자 고유 이름을 두지 않는다),
    // 판정 전체 시간 상한을 지키도록 요청 제한 시간을 호출하는 쪽이 정한다.
    String marketStatusRaw(String statusPath, Duration timeout) {
        return withTokenRetry(bearer -> sendRaw(statusPath, mapper.createObjectNode(), bearer, timeout));
    }

    // post() 와 같은 요청 형식이며, 성공 여부만 확인하고 응답 본문은 그대로 돌려준다.
    private String postRaw(String path, ObjectNode dataBody) {
        return withTokenRetry(bearer -> sendRaw(path, dataBody, bearer, Duration.ofSeconds(properties.timeoutSeconds())));
    }

    private String sendRaw(String path, ObjectNode dataBody, String bearer, Duration timeout) {
        if (isBlank(properties.baseUrl()) || isBlank(properties.appKey()) || isBlank(properties.appSecret())) {
            throw new BrokerApiException(FailureType.TOKEN_ISSUE_FAILED,
                    "BROKER_BASE_URL / BROKER_APP_KEY / BROKER_APP_SECRET 이 설정되지 않았습니다");
        }
        ObjectNode request = mapper.createObjectNode();
        ObjectNode header = request.putObject("dataHeader");
        header.put("ipAddr", HEADER_IP);
        header.put("macAddr", HEADER_MAC);
        request.set("dataBody", dataBody);

        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(properties.baseUrl() + path))
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + bearer)
                .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request), StandardCharsets.UTF_8))
                .build();
        try {
            HttpResponse<String> response = http.send(httpRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() == HTTP_UNAUTHORIZED) {
                // 거부 응답은 본문이 JSON 이 아닐 수 있어 파싱하기 전에 걸러야 재발급 재시도가 동작한다.
                throw new BrokerApiException(FailureType.HTTP_ERROR, "시세 API 오류 http=" + HTTP_UNAUTHORIZED,
                        HTTP_UNAUTHORIZED, null, null, null);
            }
            JsonNode responseHeader = mapper.readTree(response.body()).path("dataHeader");
            if (response.statusCode() != 200 || !RESULT_OK.equals(responseHeader.path("resultCode").asString(""))) {
                // 서버 문구는 외부 원문이라 남기지 않고 원인 구분용 코드만 남긴다.
                throw new BrokerApiException(
                        response.statusCode() != 200 ? FailureType.HTTP_ERROR : FailureType.RESULT_CODE_FAILED,
                        "시세 API 오류 http=" + response.statusCode()
                                + " processCode=" + safeCode(responseHeader.path("processCode").asString("")),
                        response.statusCode(), null, null, null);
            }
            return response.body().trim();
        } catch (tools.jackson.core.JacksonException e) {
            // 파싱 예외 메시지에 응답 일부가 들어갈 수 있어 종류만 남긴다.
            throw new BrokerApiException(FailureType.RESULT_CODE_FAILED, "시세 API 응답 형식 오류");
        } catch (IOException e) {
            // 원인 예외 메시지에 요청 정보가 들어갈 수 있어 연결하지 않는다.
            FailureType type = e instanceof HttpTimeoutException ? FailureType.TIMEOUT : FailureType.CONNECTION_FAILED;
            throw new BrokerApiException(type, "시세 API 호출 실패: " + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 중단");
        }
    }
}

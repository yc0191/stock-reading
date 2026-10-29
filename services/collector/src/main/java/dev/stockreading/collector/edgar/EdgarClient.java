package dev.stockreading.collector.edgar;

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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * SEC EDGAR 조회 클라이언트. 인증키는 없고 User-Agent 선언이 필요하다(값은 설정에서만 읽고 어디에도 남기지 않는다).
 * 모든 호출이 한 간격 장치를 거쳐 초당 상한을 넘지 않는다.
 */
@Component
class EdgarClient implements EdgarPort {

    static final Duration TICKER_MAP_TTL = Duration.ofHours(24);
    private static final Pattern CIK = Pattern.compile("[0-9]{10}");

    private final EdgarProperties properties;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final EdgarCallSpacing spacing;
    private final HttpClient http;

    private Map<String, String> cikByTicker;
    private Instant tickerMapLoadedAt;

    // 생성자는 하나만 둔다. 둘 이상이면 스프링이 어느 것으로 만들지 정하지 못해 수집기가 시작하지 못한다.
    EdgarClient(EdgarProperties properties, ObjectMapper mapper, Clock clock, EdgarCallSpacing spacing) {
        this.properties = properties;
        this.mapper = mapper;
        this.clock = clock;
        this.spacing = spacing;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    @Override
    public String cikOf(String ticker) {
        String cik = tickerMap().get(normalize(ticker));
        if (cik == null) {
            throw new EdgarApiException(EdgarFailure.UNKNOWN_TICKER, null);
        }
        return cik;
    }

    @Override
    public CompanySubmissions submissions(String cik) {
        if (cik == null || !CIK.matcher(cik).matches()) {
            throw new IllegalArgumentException("cik 는 10자리 숫자여야 합니다");
        }
        JsonNode body = getJson(properties.baseUrl() + "/submissions/CIK" + cik + ".json");
        return SubmissionsParser.parse(body, cik, properties.archivesBaseUrl());
    }

    // 매핑 파일은 하루에 한 번만 받는다. 종목마다 받으면 호출이 두 배가 된다.
    private synchronized Map<String, String> tickerMap() {
        Instant now = clock.instant();
        if (cikByTicker != null && now.isBefore(tickerMapLoadedAt.plus(TICKER_MAP_TTL))) {
            return cikByTicker;
        }
        JsonNode body = getJson(properties.tickerMapUrl());
        if (!body.isObject()) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, null);
        }
        Map<String, String> map = new HashMap<>();
        for (JsonNode entry : body) {
            String ticker = entry.path("ticker").asString("");
            long cik = entry.path("cik_str").asLong(-1);
            if (!ticker.isBlank() && cik > 0) {
                // 같은 티커가 두 번 나오면 먼저 나온 것을 쓴다(파일은 시가총액 순이다).
                map.putIfAbsent(normalize(ticker), String.format("%010d", cik));
            }
        }
        if (map.isEmpty()) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, null);
        }
        cikByTicker = map;
        tickerMapLoadedAt = now;
        return map;
    }

    // SEC 매핑 파일은 종류 구분을 하이픈으로 쓴다(예: XXX-B). 설정은 점도 허용하므로 하이픈으로 맞춘다.
    static String normalize(String ticker) {
        return ticker == null ? "" : ticker.trim().toUpperCase(Locale.ROOT).replace('.', '-');
    }

    private JsonNode getJson(String url) {
        if (!properties.hasUserAgent()) {
            throw new EdgarApiException(EdgarFailure.NOT_CONFIGURED, null);
        }
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(properties.timeoutSeconds()))
                .header("User-Agent", properties.userAgent())
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            spacing.await();
            response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (HttpTimeoutException e) {
            throw new EdgarApiException(EdgarFailure.TIMEOUT, null);
        } catch (IOException e) {
            // 원인 예외는 연결하지 않는다. 메시지에 요청 정보가 섞일 수 있다.
            throw new EdgarApiException(EdgarFailure.NETWORK, null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EdgarApiException(EdgarFailure.INTERRUPTED, null);
        }
        int status = response.statusCode();
        if (status != 200) {
            throw new EdgarApiException(classify(status), status);
        }
        try {
            return mapper.readTree(response.body());
        } catch (RuntimeException e) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, status);
        }
    }

    static EdgarFailure classify(int status) {
        if (status == 404) {
            return EdgarFailure.NOT_FOUND;
        }
        if (status == 403) {
            return EdgarFailure.FORBIDDEN;
        }
        if (status == 429) {
            return EdgarFailure.RATE_LIMITED;
        }
        if (status >= 500) {
            return EdgarFailure.SERVER_ERROR;
        }
        return EdgarFailure.CLIENT_ERROR;
    }
}

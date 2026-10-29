package dev.stockreading.collector.edgar;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * SEC EDGAR 조회 클라이언트. 인증키는 없고 User-Agent 선언이 필요하다(값은 설정에서만 읽고 어디에도 남기지 않는다).
 * 모든 호출이 한 간격 장치를 거쳐 초당 상한을 넘지 않는다.
 * submissions 는 조건 조회가 없어 매번 통째로 오므로 gzip 압축을 요청한다(회사당 약 167KB가 약 22KB로 준다).
 */
@Component
class EdgarClient implements EdgarPort {

    static final Duration TICKER_MAP_TTL = Duration.ofHours(24);
    // 매핑 파일 받기에 실패하면 이 시간 동안은 다시 받지 않는다. 막힌 SEC 를 종목마다 두드리지 않기 위해서다.
    static final Duration TICKER_MAP_RETRY_AFTER = Duration.ofMinutes(10);
    private static final Pattern CIK = Pattern.compile("[0-9]{10}");
    private static final Logger log = LoggerFactory.getLogger(EdgarClient.class);

    private final EdgarProperties properties;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final EdgarCallSpacing spacing;
    private final HttpClient http;

    private Map<String, Company> companies;
    private Instant tickerMapLoadedAt;
    private Instant lastFailureAt;
    private EdgarApiException lastFailure;

    // 티커 하나의 회사 번호와 상장 거래소(SEC 값 그대로, 없으면 null).
    private record Company(String cik, String exchange) {
    }

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
        return company(ticker).cik();
    }

    @Override
    public List<String> exchangesOf(String ticker) {
        String exchange = company(ticker).exchange();
        return exchange == null ? List.of() : List.of(exchange);
    }

    private Company company(String ticker) {
        Company company = tickerMap().get(normalize(ticker));
        if (company == null) {
            throw new EdgarApiException(EdgarFailure.UNKNOWN_TICKER, null);
        }
        return company;
    }

    @Override
    public CompanySubmissions submissions(String cik) {
        if (cik == null || !CIK.matcher(cik).matches()) {
            throw new IllegalArgumentException("cik 는 10자리 숫자여야 합니다");
        }
        JsonNode body = getJson(properties.baseUrl() + "/submissions/CIK" + cik + ".json");
        return SubmissionsParser.parse(body, cik, properties.archivesBaseUrl());
    }

    // 티커·회사 번호·거래소를 한 파일로 하루에 한 번만 받는다. 거래소를 회사마다 submissions 로 확인하면
    // 공시 수집과 같은 이력을 한 번 더 받게 된다. 새로 받기에 실패하면 전에 받은 파일을 계속 써서
    // SEC 가 잠깐 막혀도 해외 시세의 거래소 선택이 멈추지 않게 한다.
    private synchronized Map<String, Company> tickerMap() {
        Instant now = clock.instant();
        if (companies != null && now.isBefore(tickerMapLoadedAt.plus(TICKER_MAP_TTL))) {
            return companies;
        }
        if (lastFailure != null && now.isBefore(lastFailureAt.plus(TICKER_MAP_RETRY_AFTER))) {
            if (companies != null) {
                return companies;
            }
            throw new EdgarApiException(lastFailure.failure(), lastFailure.httpStatus());
        }
        try {
            Map<String, Company> map = loadTickerMap();
            companies = map;
            tickerMapLoadedAt = now;
            lastFailure = null;
            return map;
        } catch (EdgarApiException e) {
            // 설정이 비어 있으면 호출 자체를 하지 않았으므로 기다릴 이유가 없다.
            if (e.failure() != EdgarFailure.NOT_CONFIGURED) {
                lastFailure = e;
                lastFailureAt = now;
            }
            if (companies != null && e.failure() != EdgarFailure.NOT_CONFIGURED) {
                log.warn("edgar ticker map refresh failed, keeping the previous map failure={} http={}",
                        e.failure(), e.httpStatus());
                return companies;
            }
            throw e;
        }
    }

    // company_tickers_exchange.json 은 {"fields":["cik","name","ticker","exchange"],"data":[[...],...]} 모양이다.
    // 열 순서가 바뀌어도 읽도록 fields 에서 위치를 찾는다.
    private Map<String, Company> loadTickerMap() {
        JsonNode body = getJson(properties.tickerMapUrl());
        JsonNode fields = body.path("fields");
        JsonNode data = body.path("data");
        if (!fields.isArray() || !data.isArray()) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, null);
        }
        int cikAt = -1;
        int tickerAt = -1;
        int exchangeAt = -1;
        for (int i = 0; i < fields.size(); i++) {
            switch (fields.get(i).asString("")) {
                case "cik" -> cikAt = i;
                case "ticker" -> tickerAt = i;
                case "exchange" -> exchangeAt = i;
                default -> {
                }
            }
        }
        if (cikAt < 0 || tickerAt < 0 || exchangeAt < 0) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, null);
        }
        Map<String, Company> map = new HashMap<>();
        for (JsonNode row : data) {
            String ticker = row.path(tickerAt).asString("");
            long cik = row.path(cikAt).asLong(-1);
            JsonNode exchangeNode = row.path(exchangeAt);
            String exchange = exchangeNode.isNull() || exchangeNode.isMissingNode() ? "" : exchangeNode.asString("").trim();
            if (!ticker.isBlank() && cik > 0) {
                // 같은 티커가 두 번 나오면 먼저 나온 것을 쓴다(파일은 시가총액 순이다).
                map.putIfAbsent(normalize(ticker),
                        new Company(String.format("%010d", cik), exchange.isEmpty() ? null : exchange));
            }
        }
        if (map.isEmpty()) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, null);
        }
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
                .header("Accept-Encoding", "gzip")
                .GET()
                .build();
        HttpResponse<byte[]> response;
        try {
            spacing.await();
            response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
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
            return mapper.readTree(decode(response));
        } catch (IOException | RuntimeException e) {
            throw new EdgarApiException(EdgarFailure.INVALID_RESPONSE, status);
        }
    }

    // 압축을 요청해도 서버가 압축하지 않고 줄 수 있으므로 Content-Encoding 을 보고 푼다.
    static String decode(HttpResponse<byte[]> response) throws IOException {
        byte[] body = response.body();
        boolean gzip = response.headers().firstValue("Content-Encoding")
                .map(value -> value.trim().equalsIgnoreCase("gzip"))
                .orElse(false);
        if (!gzip) {
            return new String(body, StandardCharsets.UTF_8);
        }
        try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(body))) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
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

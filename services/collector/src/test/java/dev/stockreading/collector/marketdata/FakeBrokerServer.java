package dev.stockreading.collector.marketdata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * 시세 제공자를 흉내 내는 로컬 가짜 서버. 응답 상태, 본문, 지연을 테스트가 바꾼다. 실제 서버는 호출하지 않는다.
 * 수집기 타이머 흐름 테스트(schedule 패키지)도 쓰므로 그 테스트가 쓰는 것만 공개한다.
 */
public final class FakeBrokerServer implements AutoCloseable {

    static final String OK_HEADER =
            "{\"resultCode\":\"200\",\"processCode\":\"0024\",\"processTime\":\"20261022101530123\"}";

    private static final Pattern SYMBOL = Pattern.compile("\"shrt_cd\"\\s*:\\s*\"([^\"]+)\"");

    final AtomicInteger tokenCalls = new AtomicInteger();
    public final AtomicInteger priceCalls = new AtomicInteger();
    final AtomicInteger overseasCalls = new AtomicInteger();
    // 국내 일봉, 세계지수, 환율종합, 시장종합 호출을 합쳐 센다.
    final AtomicInteger insightCalls = new AtomicInteger();
    final AtomicInteger marketStatusCalls = new AtomicInteger();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();
    final List<String> authHeaders = new CopyOnWriteArrayList<>();

    volatile int tokenStatus = 200;
    volatile String tokenBody = "{\"dataHeader\":" + OK_HEADER
            + ",\"dataBody\":{\"access_token\":\"tok-secret-1\",\"token_type\":\"Bearer\",\"expires_in\":86400}}";
    volatile long tokenDelayMs;
    // true 이면 발급할 때마다 다른 토큰(tok-secret-N)을 준다. 재발급된 토큰이 쓰였는지 확인할 때 켠다.
    volatile boolean rotateTokens;
    volatile int priceStatus = 200;
    volatile String priceBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"now_prc\":\"10250\"}}";
    public volatile long priceDelayMs;
    volatile int marketStatusStatus = 200;
    volatile long marketStatusDelayMs;
    // 장 운영 상태 조회에 쓰는 가짜 경로와 응답 칸 이름. 실제 제공자의 이름은 코드에 두지 않는다.
    static final String MARKET_STATUS_PATH = "/api/v1/market-status";
    static final MarketStatusProperties MARKET_STATUS = new MarketStatusProperties(MARKET_STATUS_PATH,
            "today_field", "business_day_field", null);
    // 앞으로 장 운영 상태 조회를 500 으로 실패시킬 횟수. 그 뒤에는 marketStatusStatus 를 따른다.
    final AtomicInteger failMarketStatusCalls = new AtomicInteger();
    // 평일(영업일) 응답. 두 일자가 같으면 영업일이다.
    volatile String marketStatusBody = marketStatus("20261110", "20261110");
    // 인증이 필요한 호출(시세, 장 운영 상태)에 앞으로 401 을 줄 횟수. 토큰이 거부된 상황을 흉내 낸다.
    final AtomicInteger rejectAuthorizedCalls = new AtomicInteger();
    volatile String rejectBody = "Unauthorized";
    // 설정하면 이 Authorization 값으로 온 요청만 거부 대상이 된다.
    volatile String rejectOnlyBearer;
    // 종목 코드별 현재가 응답 본문. 없으면 priceBody 를 쓴다.
    final Map<String, String> priceBodyBySymbol = new ConcurrentHashMap<>();
    // 설정하면 다른 현재가 요청과 겹쳐 처리 중인 요청에는 종목과 상관없이 이 본문을 보낸다. 동시 호출 때만 깨지는 서버를 흉내 낸다.
    volatile String overlapPriceBody;

    static String marketStatus(String nowDt, String standardBusinessDt) {
        return "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"today_field\":\"" + nowDt
                + "\",\"business_day_field\":\"" + standardBusinessDt + "\",\"state_field\":\"open\"}}";
    }

    private final HttpServer server;

    public FakeBrokerServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/oauth2/token", exchange -> {
            tokenCalls.incrementAndGet();
            sleep(tokenDelayMs);
            String body = rotateTokens
                    ? tokenBody.replace("tok-secret-1", "tok-secret-" + tokenCalls.get())
                    : tokenBody;
            reply(exchange, tokenStatus, body);
        });
        server.createContext("/api/v1/ivu10140", exchange -> {
            priceCalls.incrementAndGet();
            authHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            if (rejectIfNeeded(exchange)) {
                return;
            }
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                sleep(priceDelayMs);
                String request = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Matcher symbol = SYMBOL.matcher(request);
                String body = symbol.find() ? priceBodyBySymbol.getOrDefault(symbol.group(1), priceBody) : priceBody;
                String overlap = overlapPriceBody;
                if (overlap != null && inFlight.get() > 1) {
                    body = overlap;
                }
                reply(exchange, priceStatus, body);
            } finally {
                inFlight.decrementAndGet();
            }
        });
        server.createContext("/api/v1/market-status", exchange -> {
            marketStatusCalls.incrementAndGet();
            sleep(marketStatusDelayMs);
            authHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            if (rejectIfNeeded(exchange)) {
                return;
            }
            boolean fail = failMarketStatusCalls.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0;
            reply(exchange, fail ? 500 : marketStatusStatus, marketStatusBody);
        });
        server.createContext("/api/v1/gsc10060", exchange -> {
            overseasCalls.incrementAndGet();
            reply(exchange, 200, "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"out2\":[]}}");
        });
        for (String path : List.of("/api/v1/ivs11560", "/api/v1/iva60140", "/api/v1/iva60190", "/api/v1/ivsa0070")) {
            server.createContext(path, exchange -> {
                insightCalls.incrementAndGet();
                reply(exchange, 200, "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{}}");
            });
        }
        server.start();
    }

    public String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private boolean rejectIfNeeded(HttpExchange exchange) throws IOException {
        String only = rejectOnlyBearer;
        if (only != null && !only.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
            return false;
        }
        if (rejectAuthorizedCalls.get() > 0 && rejectAuthorizedCalls.getAndDecrement() > 0) {
            reply(exchange, 401, rejectBody);
            return true;
        }
        return false;
    }

    private static void sleep(long millis) {
        if (millis > 0) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void reply(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}

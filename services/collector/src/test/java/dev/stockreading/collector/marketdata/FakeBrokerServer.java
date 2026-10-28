package dev.stockreading.collector.marketdata;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/** 시세 제공자를 흉내 내는 로컬 가짜 서버. 응답 상태, 본문, 지연을 테스트가 바꾼다. 실제 서버는 호출하지 않는다. */
final class FakeBrokerServer implements AutoCloseable {

    static final String OK_HEADER =
            "{\"resultCode\":\"200\",\"processCode\":\"0024\",\"processTime\":\"20261022101530123\"}";

    final AtomicInteger tokenCalls = new AtomicInteger();
    final AtomicInteger priceCalls = new AtomicInteger();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();
    final List<String> authHeaders = new CopyOnWriteArrayList<>();

    volatile int tokenStatus = 200;
    volatile String tokenBody = "{\"dataHeader\":" + OK_HEADER
            + ",\"dataBody\":{\"access_token\":\"tok-secret-1\",\"token_type\":\"Bearer\",\"expires_in\":86400}}";
    volatile long tokenDelayMs;
    volatile int priceStatus = 200;
    volatile String priceBody = "{\"dataHeader\":" + OK_HEADER + ",\"dataBody\":{\"now_prc\":\"10250\"}}";
    volatile long priceDelayMs;

    private final HttpServer server;

    FakeBrokerServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/oauth2/token", exchange -> {
            tokenCalls.incrementAndGet();
            sleep(tokenDelayMs);
            reply(exchange, tokenStatus, tokenBody);
        });
        server.createContext("/api/v1/ivu10140", exchange -> {
            priceCalls.incrementAndGet();
            authHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                sleep(priceDelayMs);
                reply(exchange, priceStatus, priceBody);
            } finally {
                inFlight.decrementAndGet();
            }
        });
        server.start();
    }

    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
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

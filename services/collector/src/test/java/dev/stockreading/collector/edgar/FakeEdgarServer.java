package dev.stockreading.collector.edgar;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;

/** 가짜 SEC 서버. 티커 매핑 파일과 submissions 를 돌려주고, 받은 User-Agent 와 호출 수를 기록한다. 값은 모두 가짜다. */
final class FakeEdgarServer implements AutoCloseable {

    static final String USER_AGENT = "TestCompany test-contact@example.com";

    static final String TICKER_MAP = """
            {"0":{"cik_str":1,"ticker":"TESTA","title":"Test A Corp"},
             "1":{"cik_str":2,"ticker":"TESTB-C","title":"Test B Corp"},
             "2":{"cik_str":2,"ticker":"TESTB","title":"Test B Corp"}}""";

    // 8-K(항목 둘), 6-K(항목 없음), 10-Q, 주 문서가 빈 8-K/A, 접수 시각이 깨진 행.
    static final String SUBMISSIONS = """
            {"cik":"0000000001","tickers":["TESTA"],"exchanges":["Nasdaq",""],
             "filings":{"recent":{
               "accessionNumber":["0000000001-26-000003","0000000001-26-000002","0000000001-26-000001","0000000001-26-000004","0000000001-26-000005"],
               "filingDate":["2026-10-20","2026-10-19","2026-10-01","2026-10-20","2026-10-20"],
               "reportDate":["2026-10-20","","2026-09-30","",""],
               "acceptanceDateTime":["2026-10-20T20:30:28.000Z","2026-10-19T11:02:03.000Z","2026-10-01T21:00:00.000Z","2026-10-20T21:00:00.000Z","yesterday"],
               "form":["8-K","6-K","10-Q","8-K/A","8-K"],
               "items":["2.02,9.01","","","5.02",""],
               "primaryDocument":["test-8k.htm","test-6k.htm","test-10q.htm","",""]
             },"files":[]}}""";

    final HttpServer server;
    final AtomicInteger tickerMapCalls = new AtomicInteger();
    final AtomicInteger submissionsCalls = new AtomicInteger();
    final List<String> userAgents = new CopyOnWriteArrayList<>();
    final List<String> paths = new CopyOnWriteArrayList<>();
    volatile int tickerMapStatus = 200;
    volatile String tickerMapBody = TICKER_MAP;
    volatile int submissionsStatus = 200;
    volatile String submissionsBody = SUBMISSIONS;

    FakeEdgarServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/files/company_tickers.json", exchange -> {
            tickerMapCalls.incrementAndGet();
            userAgents.add(exchange.getRequestHeaders().getFirst("User-Agent"));
            respond(exchange, tickerMapStatus, tickerMapBody);
        });
        server.createContext("/submissions/", exchange -> {
            submissionsCalls.incrementAndGet();
            userAgents.add(exchange.getRequestHeaders().getFirst("User-Agent"));
            paths.add(exchange.getRequestURI().getPath());
            respond(exchange, submissionsStatus, submissionsBody);
        });
        server.start();
    }

    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    EdgarProperties properties(String userAgent) {
        return new EdgarProperties(baseUrl(), baseUrl() + "/files/company_tickers.json",
                "https://www.sec.gov/Archives/edgar/data", userAgent, 5, 10, "8-K,8-K/A,6-K", 72);
    }

    private static void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body)
            throws IOException {
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

package dev.stockreading.collector.marketdata;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

// 현재가 조회의 호출 한도를 알아보는 수동 도구다. BROKER_RATE_PROBE=1 일 때만 동작한다.
// 키를 .env 같은 사본에서 가져와 틀린 값으로 서버를 반복 호출한 일이 있어, scripts/live_probe.py 가 켜 주는 LIVE_PROBE_GUARD 가
// 있을 때만 실행한다. 그 도구가 Parameter Store 에서 키를 읽고 직전 거절 기록을 확인한다.
// 실제 운영 서버를 호출하므로 속도를 낮은 값부터 천천히 올리고, 정상이 아닌 응답이 한 번이라도 나오면 바로 멈춘다.
// 정상 응답의 processCode 는 BROKER_RATE_PROBE_OK_CODE 로 받는다(코드에 값을 두지 않는다).
// 속도는 BROKER_RATE_PROBE_RATES(쉼표 구분, 기본 1,2,3), 단계 시간은 BROKER_RATE_PROBE_PHASE_SECONDS(기본 30)로 바꾼다.
// 토큰은 한 번만 발급해 재사용한다. 시세 값은 기록하지 않고 단계별 횟수·지연·처음 이상 응답의 헤더만 BROKER_PROBE_OUT 에 쓴다.
@EnabledIfEnvironmentVariable(named = "BROKER_RATE_PROBE", matches = "1")
@EnabledIfEnvironmentVariable(named = "LIVE_PROBE_GUARD", matches = "1",
        disabledReason = "scripts/live_probe.py 로만 실행한다(키는 Parameter Store 에서 읽고 직전 거절 기록을 확인함)")
class BrokerRateProbeTest {

    private static final long REST_MILLIS = 30_000;
    private static final long SLOW_MILLIS = 5_000;

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final AtomicBoolean stop = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    private Path out;
    private String okCode;
    private long phaseMillis;

    private record Outcome(int http, String resultCode, String processCode, long millis, String header) {
        boolean abnormal(String okCode) {
            return http != 200 || !"200".equals(resultCode) || !okCode.equals(processCode) || millis > SLOW_MILLIS;
        }
    }

    @Test
    void probe() throws Exception {
        String baseUrl = System.getenv("BROKER_BASE_URL");
        String symbol = System.getenv("BROKER_PROBE_SYMBOLS").split(",")[0].trim();
        out = Path.of(System.getenv("BROKER_PROBE_OUT"));
        okCode = System.getenv("BROKER_RATE_PROBE_OK_CODE");
        Files.writeString(out, "", StandardCharsets.UTF_8);
        int[] rates = java.util.Arrays.stream(System.getenv().getOrDefault("BROKER_RATE_PROBE_RATES", "1,2,3").split(","))
                .mapToInt(s -> Integer.parseInt(s.trim())).toArray();
        phaseMillis = Long.parseLong(System.getenv().getOrDefault("BROKER_RATE_PROBE_PHASE_SECONDS", "30")) * 1000;

        String token = issueToken(baseUrl);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            for (int i = 0; i < rates.length && !stop.get(); i++) {
                runPhase(scheduler, baseUrl, token, symbol, rates[i]);
                if (!stop.get() && i < rates.length - 1) {
                    log("rest " + REST_MILLIS / 1000 + "s");
                    Thread.sleep(REST_MILLIS);
                }
            }
            log(stop.get() ? "RESULT: 이상 응답으로 중단" : "RESULT: 모든 단계에서 이상 없음");
        } finally {
            scheduler.shutdownNow();
        }
    }

    private void runPhase(ScheduledExecutorService scheduler, String baseUrl, String token, String symbol, int rate)
            throws Exception {
        List<Outcome> outcomes = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger sent = new AtomicInteger();
        long period = 1000L / rate;
        long total = phaseMillis / period;
        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(() -> {
            if (stop.get() || sent.get() >= total) {
                return;
            }
            sent.incrementAndGet();
            inFlight.incrementAndGet();
            sendPrice(baseUrl, token, symbol, outcomes);
        }, 0, period, TimeUnit.MILLISECONDS);

        while (!stop.get() && sent.get() < total) {
            Thread.sleep(50);
        }
        task.cancel(false);
        for (int waited = 0; inFlight.get() > 0 && waited < 150; waited++) {
            Thread.sleep(100);
        }

        List<Outcome> snapshot;
        synchronized (outcomes) {
            snapshot = new ArrayList<>(outcomes);
        }
        long okCount = snapshot.stream().filter(o -> !o.abnormal(okCode)).count();
        long max = snapshot.stream().mapToLong(Outcome::millis).max().orElse(0);
        double avg = snapshot.stream().mapToLong(Outcome::millis).average().orElse(0);
        log(String.format("phase %d/s: 계획=%d 전송=%d 응답=%d 정상=%d 평균=%.0fms 최대=%dms",
                rate, total, sent.get(), snapshot.size(), okCount, avg, max));
        snapshot.stream().filter(o -> o.abnormal(okCode)).findFirst().ifPresent(o -> {
            stop.set(true);
            log("FIRST ABNORMAL: http=" + o.http() + " " + o.millis() + "ms header=" + o.header());
        });
    }

    private void sendPrice(String baseUrl, String token, String symbol, List<Outcome> outcomes) {
        ObjectNode body = mapper.createObjectNode();
        body.put("excg_clsf", "0");
        body.put("shrt_cd", symbol);
        long start = System.nanoTime();
        http.sendAsync(request(baseUrl + "/api/v1/ivu10140", body, token), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .whenComplete((response, error) -> {
                    long millis = (System.nanoTime() - start) / 1_000_000;
                    Outcome outcome;
                    if (error != null) {
                        outcome = new Outcome(-1, "", "", millis, describe(error));
                    } else {
                        // 시세 값이 섞이지 않도록 응답 본문에서 헤더만 꺼낸다.
                        JsonNode header = mapper.readTree(response.body()).path("dataHeader");
                        outcome = new Outcome(response.statusCode(), header.path("resultCode").asString(""),
                                header.path("processCode").asString(""), millis, mapper.writeValueAsString(header));
                    }
                    outcomes.add(outcome);
                    if (outcome.abnormal(okCode)) {
                        stop.set(true);
                    }
                    inFlight.decrementAndGet();
                });
    }

    private String issueToken(String baseUrl) throws Exception {
        ObjectNode body = mapper.createObjectNode();
        body.put("appKey", System.getenv("BROKER_APP_KEY"));
        body.put("appSecret", System.getenv("BROKER_APP_SECRET"));
        body.put("grantType", "client_credentials");
        HttpResponse<String> response = http.send(request(baseUrl + "/oauth2/token", body, null),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        JsonNode json = mapper.readTree(response.body());
        String token = json.path("dataBody").path("access_token").asString("");
        log("token: http=" + response.statusCode() + " processCode=" + json.path("dataHeader").path("processCode").asString("")
                + " expires_in=" + json.path("dataBody").path("expires_in"));
        if (token.isBlank()) {
            throw new IllegalStateException("토큰을 받지 못해 중단합니다");
        }
        return token;
    }

    private HttpRequest request(String url, ObjectNode dataBody, String token) {
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
        return builder.build();
    }

    // 응답을 받지 못했을 때 서버가 끊었는지, 이 컴퓨터의 연결 문제인지 가리려고 원인 사슬의 종류와 가장 안쪽 메시지를 남긴다.
    // 요청 헤더(토큰)는 예외 메시지에 들어가지 않는다.
    private static String describe(Throwable error) {
        StringBuilder chain = new StringBuilder();
        Throwable cause = error;
        Throwable innermost = error;
        for (int depth = 0; cause != null && depth < 6; depth++) {
            if (depth > 0) {
                chain.append(" <- ");
            }
            chain.append(cause.getClass().getSimpleName());
            innermost = cause;
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        String message = innermost.getMessage();
        if (message != null && !message.isBlank()) {
            chain.append(" | ").append(message.length() > 120 ? message.substring(0, 120) : message);
        }
        return chain.toString();
    }

    private synchronized void log(String line) {
        try {
            Files.writeString(out, line + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

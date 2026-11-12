package dev.stockreading.collector.marketdata;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import tools.jackson.databind.json.JsonMapper;

// 실제 서버에서 합쳐진 BrokerClient 가 시세 조회와 마감 후 조회를 한 번 발급한 토큰으로 처리하는지 보는 수동 도구다.
// scripts/live_probe.py --probe smoke 로만 실행한다(LIVE_PROBE_GUARD). 토큰 1회, 현재가 1회, 마감 후 조회 2회만 호출한다.
// 값은 기록하지 않고 호출별 성공 여부와 응답 길이만 BROKER_PROBE_OUT 에 쓴다.
@EnabledIfEnvironmentVariable(named = "BROKER_SMOKE", matches = "1")
@EnabledIfEnvironmentVariable(named = "LIVE_PROBE_GUARD", matches = "1",
        disabledReason = "scripts/live_probe.py 로만 실행한다(키는 Parameter Store 에서 읽고 직전 거절 기록을 확인함)")
class BrokerClientLiveSmokeTest {

    private final List<String> report = new ArrayList<>();

    @Test
    void oneClientServesQuotesAndPostCloseQueriesWithOneToken() throws Exception {
        Path out = Path.of(System.getenv("BROKER_PROBE_OUT"));
        String symbol = System.getenv("BROKER_PROBE_SYMBOLS").split(",")[0].trim();
        var properties = new BrokerProperties(System.getenv("BROKER_BASE_URL"), System.getenv("BROKER_APP_KEY"),
                System.getenv("BROKER_APP_SECRET"), 10, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS);
        var client = new BrokerClient(properties, JsonMapper.builder().build(), Clock.systemDefaultZone(), CallRateLimiter.perSecond(10));
        try {
            step("price", () -> client.currentPrice(symbol) != null ? "ok" : "empty");
            step("market-summary", () -> "len=" + client.marketSummaryRaw().length());
            step("daily-price", () -> "len=" + client.dailyPriceRaw(symbol, "0", 1).length());
            BrokerClient.TokenState token = client.tokenState();
            report.add("token: reused=" + (token != null) + " ageMs=" + (token == null ? -1 : token.ageMs()));
        } finally {
            Files.writeString(out, String.join("\n", report) + "\n", StandardCharsets.UTF_8);
        }
    }

    // 실패해도 다음 호출을 이어 가며 호출별 결과를 모은다. 서버 문구는 BrokerApiException 이 이미 담지 않는다.
    private void step(String name, Supplier<String> call) {
        long start = System.nanoTime();
        try {
            String result = call.get();
            report.add("call " + name + ": OK " + result + " " + (System.nanoTime() - start) / 1_000_000 + "ms");
        } catch (BrokerApiException e) {
            report.add("call " + name + ": FAIL type=" + e.failureType() + " http=" + e.httpStatus()
                    + " code=" + e.responseCode());
        } catch (RuntimeException e) {
            report.add("call " + name + ": FAIL exception=" + e.getClass().getSimpleName());
        }
    }
}

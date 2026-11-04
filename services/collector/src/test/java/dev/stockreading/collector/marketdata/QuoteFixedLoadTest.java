package dev.stockreading.collector.marketdata;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import dev.stockreading.collector.CollectorProperties;
import tools.jackson.databind.json.JsonMapper;

// 종목 n개를 1초마다 n회씩 조회할 때 수집기가 버티는지 재는 수동 도구다. QUOTE_LOAD=1 일 때만 동작한다.
// 실제 시세 제공자는 호출하지 않고 로컬 가짜 서버(FakeBrokerServer)만 쓴다. 응답 지연은 실서버 실측에 가깝게 기본 60ms 로 둔다.
// 리스너가 하나라 tick 이 밀리면 다음 tick 이 늦게 시작되는 것과, 3초 넘게 밀린 tick 을 건너뛰는 규칙을 실제와 같게 흉내 낸다.
// 설정: QUOTE_LOAD_LEVELS(종목 수, 기본 10,20,30,50,100), QUOTE_LOAD_SECONDS(한 번의 길이, 기본 60),
//       QUOTE_LOAD_REPEATS(같은 조건 반복, 기본 3), QUOTE_LOAD_LATENCY_MS(가짜 서버 응답 지연, 기본 60), QUOTE_LOAD_OUT(결과 파일).
// 호출 상한은 종목 수와 같게 맞추되, 상한 장치의 영향을 따로 보려면 QUOTE_LOAD_CAP 으로 바꾼다.
// 결과에는 건수와 시간만 적고 시세 값은 남기지 않는다.
@EnabledIfEnvironmentVariable(named = "QUOTE_LOAD", matches = "1")
class QuoteFixedLoadTest {

    // TickListener 의 기본 건너뜀 기준과 같다.
    private static final long STALE_MILLIS = 3_000;

    private record Run(int symbols, int repeat, int ticks, int staleSkipped, long okQuotes, long failures,
            Map<String, Integer> failureKinds, long firstTickMs, List<Long> fetchMs, long maxLagMs, int maxInFlight,
            int serverCalls) {
    }

    @Test
    void measure() throws Exception {
        int[] levels = Arrays.stream(env("QUOTE_LOAD_LEVELS", "10,20,30,50,100").split(","))
                .mapToInt(s -> Integer.parseInt(s.trim())).toArray();
        int seconds = Integer.parseInt(env("QUOTE_LOAD_SECONDS", "60"));
        int repeats = Integer.parseInt(env("QUOTE_LOAD_REPEATS", "3"));
        long latencyMs = Long.parseLong(env("QUOTE_LOAD_LATENCY_MS", "60"));
        Path out = Path.of(env("QUOTE_LOAD_OUT", "quote-load.txt"));
        Files.writeString(out, "# 가짜 서버 지연 " + latencyMs + "ms, 한 번 " + seconds + "초, 같은 조건 " + repeats + "회\n",
                StandardCharsets.UTF_8);

        for (int n : levels) {
            for (int repeat = 1; repeat <= repeats; repeat++) {
                Run run = runOnce(n, repeat, seconds, latencyMs);
                Files.writeString(out, line(run) + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            }
        }
    }

    private Run runOnce(int n, int repeat, int seconds, long latencyMs) throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            server.priceDelayMs = latencyMs;
            List<String> symbols = new ArrayList<>();
            for (int i = 1; i <= n; i++) {
                symbols.add(String.format("T%05d", i));
            }
            int cap = Integer.parseInt(env("QUOTE_LOAD_CAP", "0"));
            var broker = new BrokerProperties(server.baseUrl(), "load-key", "load-secret", 10, cap > 0 ? cap : n);
            var client = new BrokerClient(broker, JsonMapper.builder().build(), Clock.systemDefaultZone());
            var marketData = new BrokerMarketData(client, new CollectorProperties(symbols, 1_000, STALE_MILLIS), broker,
                    CallRateLimiter.perSecond(broker.maxCallsPerSecond()),
                    PriceChecks.fake());

            List<Long> fetchMs = new ArrayList<>();
            Map<String, Integer> failureKinds = new TreeMap<>();
            long okQuotes = 0;
            long failures = 0;
            long maxLagMs = 0;
            long firstTickMs = -1;
            int stale = 0;
            int ticks = 0;

            long start = System.nanoTime();
            for (int k = 0; k < seconds; k++) {
                long scheduled = start + k * 1_000_000_000L;
                long now = System.nanoTime();
                if (now < scheduled) {
                    LockSupport.parkNanos(scheduled - now);
                    now = System.nanoTime();
                }
                long lagMs = Math.max(0, (now - scheduled) / 1_000_000);
                maxLagMs = Math.max(maxLagMs, lagMs);
                ticks++;
                if (lagMs >= STALE_MILLIS) {
                    stale++;
                    continue;
                }
                long begin = System.nanoTime();
                FetchResult result = marketData.fetch(symbols);
                long elapsed = (System.nanoTime() - begin) / 1_000_000;
                if (firstTickMs < 0) {
                    firstTickMs = elapsed;
                } else {
                    fetchMs.add(elapsed);
                }
                okQuotes += result.quotes().size();
                failures += result.failures().size();
                for (QuoteFailure failure : result.failures()) {
                    String kind = failure.type() + (failure.exceptionType() == null ? "" : "/" + failure.exceptionType());
                    failureKinds.merge(kind, 1, Integer::sum);
                }
            }
            return new Run(n, repeat, ticks, stale, okQuotes, failures, failureKinds, Math.max(firstTickMs, 0), fetchMs,
                    maxLagMs, server.maxInFlight.get(), server.priceCalls.get());
        }
    }

    private static String line(Run run) {
        List<Long> sorted = new ArrayList<>(run.fetchMs());
        Collections.sort(sorted);
        return "n=" + run.symbols() + " 반복=" + run.repeat()
                + " tick=" + run.ticks() + " 건너뜀(밀림)=" + run.staleSkipped()
                + " 성공=" + run.okQuotes() + " 실패=" + run.failures() + " 실패종류=" + run.failureKinds()
                + " 첫tick=" + run.firstTickMs() + "ms"
                + " 처리(2번째 tick부터) 중앙=" + percentile(sorted, 50) + " p95=" + percentile(sorted, 95)
                + " p99=" + percentile(sorted, 99) + " 최대=" + percentile(sorted, 100) + "ms"
                + " 앞20tick중앙=" + median(run.fetchMs(), 0) + " 뒤20tick중앙=" + median(run.fetchMs(), 1) + "ms"
                + " 최대지연=" + run.maxLagMs() + "ms"
                + " 서버동시최대=" + run.maxInFlight() + " 서버받은호출=" + run.serverCalls();
    }

    // 시간이 지나며 처리 시간이 늘어나는지 보려고 앞쪽(0)과 뒤쪽(1) 20개 tick 의 중앙값을 따로 낸다.
    private static long median(List<Long> inOrder, int part) {
        int size = Math.min(20, inOrder.size());
        if (size == 0) {
            return -1;
        }
        List<Long> slice = new ArrayList<>(part == 0 ? inOrder.subList(0, size)
                : inOrder.subList(inOrder.size() - size, inOrder.size()));
        Collections.sort(slice);
        return slice.get(slice.size() / 2);
    }

    private static long percentile(List<Long> sorted, int p) {
        if (sorted.isEmpty()) {
            return -1;
        }
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.stockreading.collector.CollectorProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// 직렬 비교 시험 도구(BrokerSerialProbeTest)를 가짜 서버(FakeBrokerServer)로 끝까지 리허설한다. 실제 서버는 호출하지 않는다.
// 칸 불일치는 수집기와 같은 검사(PriceFieldCheck, 가짜 칸 이름)로 판단한다.
class SerialProbeRehearsalTest {

    private static final String NORMAL = PriceChecks.body("10250", "10000", "250", "U", "2.50", "10300", "10200");
    // 현재가만 엉뚱한 값이라 세 검사가 모두 어긋난다.
    private static final String ALL_CHECKS_BROKEN = PriceChecks.body("99999", "10000", "250", "U", "2.50", "10300", "9900");

    @TempDir
    Path tempDir;

    private final ObjectMapper mapper = JsonMapper.builder().build();

    private static List<String> symbols(int n) {
        List<String> symbols = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            symbols.add(String.format("TEST%02d", i));
        }
        return symbols;
    }

    // 종목 10개 중 4개(40%)가 매번 깨진 응답: 빈 현재가, 숫자 아닌 현재가, 칸 불일치 2가지(세 검사 모두, 과반인 두 검사).
    // 검사 하나만 어긋난 응답과 하락 응답은 정상이다.
    private static void mixed(FakeBrokerServer server) {
        server.priceBody = NORMAL;
        server.priceBodyBySymbol.put("TEST01", PriceChecks.body("", "10000", "250", "U", "2.50", "10300", "10200"));
        server.priceBodyBySymbol.put("TEST02", PriceChecks.body("12a4", "10000", "250", "U", "2.50", "10300", "10200"));
        server.priceBodyBySymbol.put("TEST03", ALL_CHECKS_BROKEN);
        server.priceBodyBySymbol.put("TEST04",
                PriceChecks.body("10900", "10000", "250", "U", "2.50", "11000", "9900"));
        server.priceBodyBySymbol.put("TEST05",
                PriceChecks.body("10250", "10000", "999", "U", "2.50", "10300", "10200"));
        server.priceBodyBySymbol.put("TEST06",
                PriceChecks.body("9900", "10000", "100", "D", "-1.00", "10100", "9850"));
    }

    private SerialProbe probe(FakeBrokerServer server, List<String> symbols, int cap, PriceFieldCheck check,
            SerialProbe.Settings settings) {
        var broker = new BrokerProperties(server.baseUrl(), "TEST-key", "TEST-secret", 5, cap);
        var client = new BrokerClient(broker, mapper, Clock.systemDefaultZone());
        CallRateLimit limiter = CallRateLimiter.perSecond(cap);
        var marketData = new BrokerMarketData(client, new CollectorProperties(symbols, 1_000, 0), broker, limiter, check);
        return new SerialProbe(client, marketData, limiter, check, settings);
    }

    private SerialProbe probe(FakeBrokerServer server, List<String> symbols, int cap, SerialProbe.Settings settings) {
        return probe(server, symbols, cap, PriceChecks.fake(), settings);
    }

    private static SerialProbe.Settings settings(int rounds) {
        return new SerialProbe.Settings(rounds, 60_000, 0);
    }

    @Test
    void countsBrokenResponsesPerModeAtTheMixedRatio() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            mixed(server);
            List<String> symbols = symbols(10);
            SerialProbe.Result result = probe(server, symbols, 50, settings(3)).run(symbols);

            assertThat(result.roundsDone()).isEqualTo(3);
            assertThat(result.stopReason()).isEqualTo("none");
            for (SerialProbe.Mode mode : SerialProbe.Mode.values()) {
                SerialProbe.Tally tally = result.tallies().get(mode);
                assertThat(tally.calls).as(mode.label).isEqualTo(30);
                assertThat(tally.emptyPrice).as(mode.label).isEqualTo(3);
                assertThat(tally.notNumber).as(mode.label).isEqualTo(3);
                assertThat(tally.fieldMismatch).as(mode.label).isEqualTo(6);
                assertThat(tally.mismatchChecks).as(mode.label)
                        .isEqualTo(Map.of("CHANGE+RATE+HIGH_LOW", 3, "CHANGE+RATE", 3));
                assertThat(tally.ok).as(mode.label).isEqualTo(18);
                assertThat(tally.nonPositive + tally.rateLimited + tally.abnormal).as(mode.label).isZero();
                assertThat(tally.line(mode)).contains("brokenRate=40.00%");
            }
            assertThat(server.priceCalls.get()).isEqualTo(60);
            assertThat(server.tokenCalls.get()).isEqualTo(1);
        }
    }

    // 병렬 경로(BrokerMarketData)는 칸이 맞지 않는 응답을 FIELD_MISMATCH 실패로 돌려준다. 이것은 센 대상이라 멈추지 않는다.
    @Test
    void countsFieldMismatchFailuresWithoutStopping() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            server.priceBody = ALL_CHECKS_BROKEN;
            List<String> symbols = symbols(4);
            SerialProbe.Result result = probe(server, symbols, 50, settings(2)).run(symbols);

            assertThat(result.roundsDone()).isEqualTo(2);
            assertThat(result.stopReason()).isEqualTo("none");
            for (SerialProbe.Mode mode : SerialProbe.Mode.values()) {
                SerialProbe.Tally tally = result.tallies().get(mode);
                assertThat(tally.calls).as(mode.label).isEqualTo(8);
                assertThat(tally.fieldMismatch).as(mode.label).isEqualTo(8);
                assertThat(tally.abnormal).as(mode.label).isZero();
                assertThat(tally.line(mode)).contains("mismatchChecks={CHANGE+RATE+HIGH_LOW=8}");
            }
        }
    }

    // 동시에 처리 중인 요청이 둘 이상일 때만 칸이 맞지 않는 응답을 주면, 직렬은 하나도 깨지지 않고 병렬만 깨진다.
    @Test
    void serialModeSendsOneCallAtATimeAndIsCountedApartFromParallel() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            server.priceDelayMs = 100;
            server.priceBody = NORMAL;
            server.overlapPriceBody = ALL_CHECKS_BROKEN;
            List<String> symbols = symbols(10);
            SerialProbe.Result result = probe(server, symbols, 50, settings(2)).run(symbols);

            SerialProbe.Tally serial = result.tallies().get(SerialProbe.Mode.SERIAL);
            SerialProbe.Tally parallel = result.tallies().get(SerialProbe.Mode.PARALLEL);
            assertThat(serial.calls).isEqualTo(20);
            assertThat(serial.broken()).isZero();
            assertThat(parallel.calls).isEqualTo(20);
            assertThat(parallel.fieldMismatch).isPositive();
            assertThat(parallel.abnormal).isZero();
        }
    }

    @Test
    void skipsTheFieldCheckWhenTheCheckIsOff() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            mixed(server);
            List<String> symbols = symbols(10);
            PriceFieldCheck off = new PriceFieldCheck(PriceChecks.properties(false, "0.003"), mapper);
            SerialProbe.Settings settings = settings(1);
            SerialProbe.Result result = probe(server, symbols, 50, off, settings).run(symbols);

            SerialProbe.Tally tally = result.tallies().get(SerialProbe.Mode.SERIAL);
            assertThat(tally.fieldMismatch).isZero();
            assertThat(tally.ok).isEqualTo(8);
            assertThat(SerialProbe.summary(result, settings, symbols.size(), 50, false).get(0))
                    .contains("fieldCheck=off");
        }
    }

    // 비교 칸이 없어 판단할 검사가 없으면 수집기처럼 통과로 센다.
    @Test
    void countsResponsesWithoutTheComparedFieldsAsOk() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            List<String> symbols = symbols(4);
            SerialProbe.Result result = probe(server, symbols, 50, settings(1)).run(symbols);

            SerialProbe.Tally tally = result.tallies().get(SerialProbe.Mode.PARALLEL);
            assertThat(tally.calls).isEqualTo(4);
            assertThat(tally.ok).isEqualTo(4);
            assertThat(tally.broken()).isZero();
        }
    }

    @Test
    void countsZeroPriceApartFromNotANumber() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            server.priceBody = PriceChecks.body("0", "10000", "250", "U", "2.50", "10300", "10200");
            List<String> symbols = symbols(2);
            SerialProbe.Result result = probe(server, symbols, 50, settings(1)).run(symbols);

            for (SerialProbe.Mode mode : SerialProbe.Mode.values()) {
                assertThat(result.tallies().get(mode).nonPositive).as(mode.label).isEqualTo(2);
                assertThat(result.tallies().get(mode).notNumber).as(mode.label).isZero();
            }
        }
    }

    // 한도 초과 같은 이상 응답이 나오면 다음 방식으로 넘어가지 않고 바로 멈춘다.
    @Test
    void stopsAtTheFirstAbnormalFailure() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            server.priceStatus = 500;
            List<String> symbols = symbols(3);
            SerialProbe.Result result = probe(server, symbols, 50, settings(5)).run(symbols);

            assertThat(result.roundsDone()).isEqualTo(1);
            assertThat(result.stopReason()).contains("HTTP_ERROR");
            assertThat(result.tallies().get(SerialProbe.Mode.PARALLEL).abnormal).isEqualTo(3);
            assertThat(result.tallies().get(SerialProbe.Mode.SERIAL).calls).isZero();
            assertThat(server.priceCalls.get()).isEqualTo(3);
        }
    }

    @Test
    void stopsWithoutPriceCallsWhenTheTokenIsRejected() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            server.tokenStatus = 500;
            List<String> symbols = symbols(3);
            SerialProbe.Result result = probe(server, symbols, 50, settings(5)).run(symbols);

            assertThat(result.stopReason()).contains("TOKEN_ISSUE_FAILED");
            assertThat(server.priceCalls.get()).isZero();
        }
    }

    // 병렬은 수집기처럼 상한을 넘는 종목을 호출하지 않고 건너뛰며, 이것은 호출 수가 아니라 rateLimited 로 센다.
    @Test
    void countsSymbolsOverTheCallLimitAsRateLimitedNotAsCalls() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            List<String> symbols = symbols(8);
            SerialProbe.Result result = probe(server, symbols, 5, settings(1)).run(symbols);

            SerialProbe.Tally parallel = result.tallies().get(SerialProbe.Mode.PARALLEL);
            assertThat(parallel.calls).isEqualTo(5);
            assertThat(parallel.rateLimited).isEqualTo(3);
            SerialProbe.Tally serial = result.tallies().get(SerialProbe.Mode.SERIAL);
            assertThat(serial.calls + serial.rateLimited).isEqualTo(8);
            assertThat(server.priceCalls.get()).isEqualTo(parallel.calls + serial.calls);
        }
    }

    @Test
    void writesOnlyCountsToAPathOutsideTheRepository() throws Exception {
        try (FakeBrokerServer server = new FakeBrokerServer()) {
            mixed(server);
            List<String> symbols = symbols(10);
            SerialProbe.Settings settings = settings(2);
            SerialProbe.Result result = probe(server, symbols, 50, settings).run(symbols);

            Path out = SerialProbe.outsideRepo(tempDir.resolve("serial-probe.txt"), Path.of(""));
            SerialProbe.write(out, SerialProbe.summary(result, settings, symbols.size(), 50, true));

            String written = Files.readString(out);
            assertThat(written).contains("parallel: calls=20", "serial: calls=20", "fieldCheck=on", "stop=none",
                    "round 2:");
            assertThat(written).doesNotContain("TEST0", "TEST1", "now_prc", "10250", "99999", "TEST_BASE", "TEST-key",
                    "tok-secret");
        }
    }

    @Test
    void refusesAnOutputPathInsideTheRepository() {
        assertThatThrownBy(() -> SerialProbe.outsideRepo(Path.of("build", "serial-probe.txt"), Path.of("")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

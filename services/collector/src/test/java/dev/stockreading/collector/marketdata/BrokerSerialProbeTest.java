package dev.stockreading.collector.marketdata;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

// 같은 종목들을 병렬(지금 수집기 방식), 직렬(한 번에 하나씩), 동시 N개 제한(한 번에 최대 N개)으로 번갈아 조회해
// 깨진 응답 비율을 비교하는 실서버 확인 도구다.
// 평소 테스트 실행에서는 건너뛰고 BROKER_SERIAL_PROBE=1 일 때만 동작한다. 실제 운영 서버를 호출하므로 장중에 사람이 승인한 뒤 돌린다.
// 모든 방식이 수집기와 같은 호출 상한 장치(CallRateLimit 하나)를 거치고, 토큰 발급과 현재가 조회만 한다.
// 방식별로 호출 수와 현재가 빈 값(EMPTY_PRICE), 숫자 아님·0 이하(PRICE_PARSE_FAILED), 응답 칸 불일치(FIELD_MISMATCH) 건수를 센다.
// 칸 불일치는 모든 방식에서 수집기와 같은 검사(PriceFieldCheck)로 판단하고, 칸 이름은 수집기와 같은 BROKER_PRICE_CHECK_* 를
// application.yml 로 묶어 받는다. 그 밖의 실패가 한 번이라도 나오면 서버를 계속 두드리지 않도록 바로 멈춘다.
// 키와 주소는 BROKER_BASE_URL, BROKER_APP_KEY, BROKER_APP_SECRET, 종목은 BROKER_PROBE_SYMBOLS(쉼표 구분)로 받고 코드에 두지 않는다.
// 결과 요약은 BROKER_SERIAL_PROBE_OUT(저장소 밖 경로)에만 쓰고, 표준 출력에는 방식별 건수 줄만 남긴다.
@EnabledIfEnvironmentVariable(named = "BROKER_SERIAL_PROBE", matches = "1")
class BrokerSerialProbeTest {

    // 0.2% 수준의 깨짐을 가리려면 방식별 수천 회가 필요하다. 종목 9~10개, 응답 0.1초면 20분 안에 끝나는 라운드 수다.
    private static final String DEFAULT_ROUNDS = "350";
    private static final String DEFAULT_SECONDS = "1200";

    @Configuration
    @EnableConfigurationProperties(PriceCheckProperties.class)
    static class Wiring {
    }

    @Test
    void compareParallelAndSerial() {
        Path out = SerialProbe.outsideRepo(Path.of(System.getenv("BROKER_SERIAL_PROBE_OUT")), Path.of(""));
        List<String> symbols = Arrays.stream(System.getenv("BROKER_PROBE_SYMBOLS").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();
        var settings = new SerialProbe.Settings(
                Integer.parseInt(env("BROKER_SERIAL_PROBE_ROUNDS", DEFAULT_ROUNDS)),
                Long.parseLong(env("BROKER_SERIAL_PROBE_SECONDS", DEFAULT_SECONDS)) * 1000,
                Long.parseLong(env("BROKER_SERIAL_PROBE_PAUSE_MS", "1000")),
                Integer.parseInt(env("BROKER_SERIAL_PROBE_CONCURRENCY", "3")),
                Arrays.stream(env("BROKER_SERIAL_PROBE_MODES", "parallel,serial,limited").split(","))
                        .map(String::trim).filter(s -> !s.isEmpty())
                        .map(s -> SerialProbe.Mode.valueOf(s.toUpperCase(Locale.ROOT))).toList());

        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .run(context -> {
                    PriceCheckProperties checkProperties = context.getBean(PriceCheckProperties.class);
                    ObjectMapper mapper = JsonMapper.builder().build();
                    var priceCheck = new PriceFieldCheck(checkProperties, mapper);
                    boolean fieldCheck = checkProperties.enabled() && !checkProperties.noFieldsConfigured();

                    // 상한 값이 0 이면 BrokerProperties 가 수집기 기본값으로 바꾼다.
                    var broker = new BrokerProperties(System.getenv("BROKER_BASE_URL"), System.getenv("BROKER_APP_KEY"),
                            System.getenv("BROKER_APP_SECRET"), 10,
                            Integer.parseInt(env("BROKER_MAX_CALLS_PER_SECOND", "0")));
                    var client = new BrokerClient(broker, mapper, Clock.systemDefaultZone(), CallRateLimiter.perSecond(10));
                    CallRateLimit limiter = CallRateLimiter.perSecond(broker.maxCallsPerSecond());
                    var marketData = new BrokerMarketData(client, broker,
                            limiter, priceCheck);

                    SerialProbe.Result result = new SerialProbe(client, marketData, limiter, priceCheck, settings)
                            .run(symbols);
                    SerialProbe.write(out, SerialProbe.summary(result, settings, symbols.size(),
                            broker.maxCallsPerSecond(), fieldCheck));
                    for (SerialProbe.Mode mode : settings.modes()) {
                        String countLine = result.tallies().get(mode).line(mode);
                        System.out.println(countLine);
                    }
                    System.out.println("fieldCheck=" + (fieldCheck ? "on" : "off") + " stop=" + result.stopReason());
                });
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

package dev.stockreading.collector.marketdata;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

// 실제 서버의 장 운영 상태 응답 모양과 휴장일 판정 결과를 한 번 확인하는 수동 도구다. 평소 테스트와 test-all.sh --live 에는 들어가지 않는다.
// 토큰 1회와 장 운영 상태 2회(원문 확인, 판정)만 호출한다. 설정된 두 일자 칸의 값과 판정 결과만 BROKER_PROBE_OUT(없으면 표준 출력)에 쓰고 종목·시세 값은 쓰지 않는다.
// 경로와 칸 이름은 BROKER_MARKET_STATUS_PATH / _TODAY_FIELD / _BUSINESS_DAY_FIELD 로 받는다.
// 휴장일 규칙(두 칸이 같으면 영업일)은 평일 한 번, 주말·공휴일 한 번 불러서 확인한다.
@EnabledIfEnvironmentVariable(named = "BROKER_MARKET_STATUS_PROBE", matches = "1")
class MarketStatusLiveProbeTest {

    @Test
    void marketStatusAnswersAreReadAndJudged() throws Exception {
        var mapper = JsonMapper.builder().build();
        Clock clock = Clock.system(ZoneId.of("Asia/Seoul"));
        var properties = new BrokerProperties(System.getenv("BROKER_BASE_URL"), System.getenv("BROKER_APP_KEY"),
                System.getenv("BROKER_APP_SECRET"), 10, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS);
        var client = new BrokerClient(properties, mapper, clock, CallRateLimiter.perSecond(10));
        var status = new MarketStatusProperties(System.getenv("BROKER_MARKET_STATUS_PATH"),
                System.getenv("BROKER_MARKET_STATUS_TODAY_FIELD"),
                System.getenv("BROKER_MARKET_STATUS_BUSINESS_DAY_FIELD"), null);
        var marketDay = new BrokerMarketDay(client, CallRateLimiter.perSecond(10), status, mapper, clock);

        List<String> report = new ArrayList<>();
        JsonNode data = mapper.readTree(client.marketStatusRaw(status.path(), status.lookupTimeout())).path("dataBody");
        for (String field : List.of(status.todayField(), status.businessDayField())) {
            JsonNode found = data.findValue(field);
            report.add(field + "=" + (found == null ? "(없음)" : found.asString("")));
        }
        report.add("judged businessDay=" + marketDay.isBusinessDay() + " at " + java.time.LocalDate.now(clock));
        report.add("calls=3 (토큰 1 + 장 운영 상태 2: 원문 확인 1, 판정 1)");

        String text = String.join("\n", report) + "\n";
        String out = System.getenv("BROKER_PROBE_OUT");
        if (out == null || out.isBlank()) {
            System.out.print(text);
        } else {
            Files.writeString(Path.of(out), text, StandardCharsets.UTF_8);
        }
    }
}

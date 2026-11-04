package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.OffsetDateTime;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import dev.stockreading.market.MarketProperties;
import dev.stockreading.market.snapshot.Evaluation.Outcome;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 의 전일 종가 칸 기본값이 비어 있어 유형 2가 꺼져 있는지, 환경변수로 칸 경로를 주면 켜지는지 본다.
 * 값은 모두 가짜다.
 */
class PrevCloseDefaultsTest {

    @Configuration
    @EnableConfigurationProperties(MarketProperties.class)
    static class Wiring {
    }

    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-01-02T01:00:00Z");

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    // 전일 종가가 중첩 칸 아래에 앞 공백이 붙은 문자열로 오는 응답 모양.
    private static final String RESPONSE = "{\"fake\":{\"now\":\"10400\",\"prevClose\":\"   10000\"}}";

    private static Evaluation prevClose(MarketProperties properties, String raw) {
        MoveDetector detector = new MoveDetector(mock(PriceSnapshotRepository.class), properties, JsonMapper.builder().build());
        return detector.evaluate("TEST01", AS_OF, 10_400, raw).stream()
                .filter(e -> e.type() == DetectionType.PREV_CLOSE).findFirst().orElseThrow();
    }

    @Test
    void emptyDefaultLeavesPrevCloseOff() {
        runner().run(context ->
                assertThat(prevClose(context.getBean(MarketProperties.class), RESPONSE).outcome())
                        .isEqualTo(Outcome.UNKNOWN));
    }

    @Test
    void environmentVariableTurnsPrevCloseOn() {
        runner().withPropertyValues("MARKET_PREV_CLOSE_FIELD=fake.prevClose").run(context -> {
            Evaluation evaluation = prevClose(context.getBean(MarketProperties.class), RESPONSE);

            assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
            assertThat(evaluation.detection().previousPrice()).isEqualTo(10_000);
        });
    }

    @Test
    void emptyEnvironmentVariableKeepsPrevCloseOff() {
        // test-all.sh --live 가 이 방법으로 유형 2를 끈다.
        runner().withPropertyValues("MARKET_PREV_CLOSE_FIELD=").run(context ->
                assertThat(prevClose(context.getBean(MarketProperties.class), RESPONSE).outcome())
                        .isEqualTo(Outcome.UNKNOWN));
    }
}

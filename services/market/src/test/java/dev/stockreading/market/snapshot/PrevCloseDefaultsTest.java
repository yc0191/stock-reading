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
 * application.yml 의 전일 종가 후보 칸 기본값으로 유형 2가 켜져 있는지, 환경변수로 칸을 바꾸거나 비워서 끌 수 있는지 본다.
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

    // 기본 후보 세 칸 중 한 칸이 앞 공백이 붙은 문자열이고 한 칸은 다른 값으로 깨진 응답 모양.
    private static final String RESPONSE =
            "{\"dataBody\":{\"bdy_cls_prc\":\"   10000\",\"sprc\":\"7000\",\"krx_bdy_clpr\":\"10000\"}}";
    private static final String OTHER_RESPONSE = "{\"fake\":{\"x\":\"10000\",\"y\":\"10000\"}}";

    private static Evaluation prevClose(MarketProperties properties, String raw) {
        MoveDetector detector = new MoveDetector(mock(PriceSnapshotRepository.class), properties, JsonMapper.builder().build());
        return detector.evaluate("TEST01", AS_OF, 10_400, raw).stream()
                .filter(e -> e.type() == DetectionType.PREV_CLOSE).findFirst().orElseThrow();
    }

    @Test
    void defaultCandidateFieldsTurnPrevCloseOnWithTheMajorityValue() {
        runner().run(context -> {
            Evaluation evaluation = prevClose(context.getBean(MarketProperties.class), RESPONSE);

            assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
            assertThat(evaluation.detection().previousPrice()).isEqualTo(10_000);
        });
    }

    @Test
    void environmentVariableReplacesTheCandidateFields() {
        runner().withPropertyValues("MARKET_PREV_CLOSE_FIELDS=fake.x,fake.y").run(context -> {
            Evaluation evaluation = prevClose(context.getBean(MarketProperties.class), OTHER_RESPONSE);

            assertThat(evaluation.outcome()).isEqualTo(Outcome.DETECTED);
            assertThat(evaluation.detection().previousPrice()).isEqualTo(10_000);
        });
    }

    @Test
    void emptyEnvironmentVariableTurnsPrevCloseOff() {
        // test-all.sh --live 가 이 방법으로 유형 2를 끈다.
        runner().withPropertyValues("MARKET_PREV_CLOSE_FIELDS=").run(context -> {
            assertThat(context.getBean(MarketProperties.class).prevCloseFields()).isEmpty();
            assertThat(prevClose(context.getBean(MarketProperties.class), RESPONSE).outcome())
                    .isEqualTo(Outcome.UNKNOWN);
        });
    }
}

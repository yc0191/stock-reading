package dev.stockreading.collector.holdings;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * application.yml 과 환경변수가 실제로 설정에 묶이는지, 형식이 틀린 값이 시작 단계에서 막히는지 확인한다.
 * 가짜 값(TEST 접두)만 쓴다.
 */
class HoldingsPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(HoldingsProperties.class)
    @Import(ConfiguredHoldings.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void startsWithNoHoldingsWhenTheEnvironmentVariableIsAbsent() {
        // 셸에 실제 보유 종목이 설정돼 있어도 읽지 않게 막아, 실패 보고서에 실제 값이 찍히지 않게 한다.
        runner().withPropertyValues("ETF_US_HOLDINGS=").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(HoldingsPort.class).allTickers()).isEmpty();
        });
    }

    @Test
    void holdingsAreReadFromTheEnvironmentVariableName() {
        runner().withPropertyValues("ETF_US_HOLDINGS=TEST01=FAKEA,FAKEB;TEST02=FAKEC").run(context -> {
            assertThat(context).hasNotFailed();
            HoldingsPort port = context.getBean(HoldingsPort.class);
            assertThat(port.topTickers("TEST01")).containsExactly("FAKEA", "FAKEB");
            assertThat(port.allTickers()).containsExactly("FAKEA", "FAKEB", "FAKEC");
        });
    }

    @Test
    void malformedValueFailsStartupAndTheFailureDoesNotEchoTheValue() {
        runner().withPropertyValues("ETF_US_HOLDINGS=TEST01=FAKEA;TEST02=fakeb").run(context -> {
            assertThat(context).hasFailed();
            String messages = messagesOf(context.getStartupFailure());
            assertThat(messages).contains("2번째").contains("티커 형식 오류");
            assertThat(messages).doesNotContain("TEST02").doesNotContain("fakeb");
        });
    }

    // 원인 사슬 전체의 메시지를 모아 본다. 스프링이 감싼 예외 어디에도 설정 값이 들어가면 안 된다.
    private static String messagesOf(Throwable failure) {
        StringBuilder all = new StringBuilder();
        for (Throwable t = failure; t != null; t = t.getCause()) {
            all.append(t.getMessage()).append('\n');
        }
        return all.toString();
    }
}

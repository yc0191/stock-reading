package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 과 환경변수(HOLDINGS_FILE, 수집기와 같은 ETF_US_HOLDINGS)가 실제로 설정에 묶이는지, 어긋난 값이
 * 시작 단계에서 막히는지 확인한다. 가짜 값(TEST 접두)만 쓴다.
 */
class HoldingsPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(HoldingsProperties.class)
    @Import(ConfiguredHoldings.class)
    static class Wiring {
        @Bean
        ObjectMapper mapper() {
            return JsonMapper.builder().build();
        }
    }

    @TempDir
    Path dir;

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    private String file(String json) throws Exception {
        Path file = dir.resolve("holdings.json");
        Files.writeString(file, json, StandardCharsets.UTF_8);
        return file.toString();
    }

    @Test
    void startsWithNoHoldingsWhenBothEnvironmentVariablesAreEmpty() {
        // 셸에 실제 값이 설정돼 있어도 읽지 않게 막아, 실패 보고서에 실제 값이 찍히지 않게 한다.
        runner().withPropertyValues("HOLDINGS_FILE=", "ETF_US_HOLDINGS=").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ConfiguredHoldings.class).plan().instruments()).isEmpty();
        });
    }

    @Test
    void holdingsFileAndCollectorSettingAreReadFromTheEnvironmentVariables() throws Exception {
        String path = file("""
                [{"kind":"ETF","market":"KR","symbol":"TEST01","members":[{"market":"US","symbol":"FAKEA"}]}]
                """);
        runner().withPropertyValues("HOLDINGS_FILE=" + path, "ETF_US_HOLDINGS=TEST01=FAKEA").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ConfiguredHoldings.class).plan().memberships()).hasSize(1);
        });
    }

    @Test
    void mismatchBetweenFileAndCollectorSettingFailsStartupWithoutEchoingValues() throws Exception {
        String path = file("""
                [{"kind":"ETF","market":"KR","symbol":"TEST01","members":[{"market":"US","symbol":"FAKEA"}]}]
                """);
        runner().withPropertyValues("HOLDINGS_FILE=" + path, "ETF_US_HOLDINGS=TEST01=FAKEB").run(context -> {
            assertThat(context).hasFailed();
            String messages = messagesOf(context.getStartupFailure());
            assertThat(messages).contains("어긋납니다");
            assertThat(messages).doesNotContain("TEST01").doesNotContain("FAKEA").doesNotContain("FAKEB");
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

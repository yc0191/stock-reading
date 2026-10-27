package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** application.yml 과 환경변수가 실제로 설정에 묶이는지, 잘못된 호출 상한이 시작 단계에서 막히는지 확인한다. */
class NewsSearchPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(NewsSearchProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void dailyCallLimitDefaultsToNineteenThousandFiveHundredWhenTheEnvironmentVariableIsAbsent() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(NewsSearchProperties.class).dailyCallLimit()).isEqualTo(19_500);
        });
    }

    @Test
    void dailyCallLimitIsReadFromTheEnvironmentVariableName() {
        runner().withPropertyValues("NEWS_SEARCH_DAILY_CALL_LIMIT=100").run(context ->
                assertThat(context.getBean(NewsSearchProperties.class).dailyCallLimit()).isEqualTo(100));
    }

    @Test
    void zeroOrNegativeDailyCallLimitFailsStartup() {
        runner().withPropertyValues("NEWS_SEARCH_DAILY_CALL_LIMIT=0").run(context ->
                assertThat(context).hasFailed());
        runner().withPropertyValues("NEWS_SEARCH_DAILY_CALL_LIMIT=-5").run(context ->
                assertThat(context).hasFailed());
    }
}

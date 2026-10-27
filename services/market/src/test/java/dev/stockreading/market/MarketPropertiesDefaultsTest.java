package dev.stockreading.market;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** application.yml 의 기본값이 문서와 같은지, 환경변수 이름이 실제로 설정에 묶이는지 확인한다. */
class MarketPropertiesDefaultsTest {

    @Configuration
    @EnableConfigurationProperties(MarketProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void defaultsMatchTheDocumentedValues() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            MarketProperties properties = context.getBean(MarketProperties.class);
            assertThat(properties.windowMinutes()).isEqualTo(5);
            assertThat(properties.windowThresholdPercent()).isEqualByComparingTo("1.00");
            assertThat(properties.prevCloseField()).isEqualTo("bdy_cls_prc");
            assertThat(properties.prevCloseThresholdPercent()).isEqualByComparingTo("3.00");
        });
    }

    @Test
    void prevCloseFieldIsReadFromTheEnvironmentVariableName() {
        runner().withPropertyValues("MARKET_PREV_CLOSE_FIELD=other.field").run(context ->
                assertThat(context.getBean(MarketProperties.class).prevCloseField()).isEqualTo("other.field"));
    }
}

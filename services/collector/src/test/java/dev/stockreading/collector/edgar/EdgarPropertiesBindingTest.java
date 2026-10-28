package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** application.yml 과 환경변수가 설정에 묶이는지, 잘못된 값이 시작 단계에서 막히는지 확인한다. */
class EdgarPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(EdgarProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void defaultsCollectEightKAmendmentsAndSixKWithoutUserAgent() {
        // 셸에 실제 연락처가 설정돼 있어도 읽지 않게 비운다.
        runner().withPropertyValues("EDGAR_USER_AGENT=", "EDGAR_FORMS=8-K,8-K/A,6-K").run(context -> {
            assertThat(context).hasNotFailed();
            EdgarProperties properties = context.getBean(EdgarProperties.class);
            assertThat(properties.formSet()).containsExactly("8-K", "8-K/A", "6-K");
            assertThat(properties.hasUserAgent()).isFalse();
            assertThat(properties.maxCallsPerSecond()).isLessThanOrEqualTo(10);
            assertThat(properties.lookbackHours()).isEqualTo(72);
        });
    }

    @Test
    void formsAndUserAgentAreReadFromEnvironmentVariableNames() {
        runner().withPropertyValues("EDGAR_USER_AGENT=TestCompany test-contact@example.com", "EDGAR_FORMS= 8-k , 10-Q")
                .run(context -> {
                    EdgarProperties properties = context.getBean(EdgarProperties.class);
                    assertThat(properties.formSet()).containsExactly("8-K", "10-Q");
                    assertThat(properties.hasUserAgent()).isTrue();
                    assertThat(properties.toString()).doesNotContain("test-contact");
                });
    }

    @Test
    void emptyOrMalformedFormsFailStartup() {
        runner().withPropertyValues("EDGAR_FORMS=").run(context -> assertThat(context).hasFailed());
        runner().withPropertyValues("EDGAR_FORMS=8-K,,6-K").run(context -> assertThat(context).hasFailed());
    }

    @Test
    void callLimitAboveTheSecFairAccessLimitIsRejected() {
        assertThatThrownBy(() -> new EdgarProperties("u", "u", "u", "ua", 5, 11, "8-K", 72))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new EdgarProperties("u", "u", "u", "ua", 5, 0, "8-K", 72))
                .isInstanceOf(IllegalArgumentException.class);
    }
}

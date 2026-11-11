package dev.stockreading.market.members;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.DayOfWeek;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** application.yml 과 환경변수(MEMBERS_*)가 실제로 설정에 묶이는지 확인한다. 가짜 값(TEST 접두)만 쓴다. */
class MemberSourcePropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(MemberSourceProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void isOffWhenTheTemplateEnvironmentVariableIsEmptyAndKeepsTheDocumentedDefaults() {
        // 셸에 실제 값이 설정돼 있어도 읽지 않게 막는다.
        runner().withPropertyValues("MEMBERS_URL_TEMPLATE=", "MEMBERS_USER_AGENT=stock-reading-members/1.0")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    MemberSourceProperties properties = context.getBean(MemberSourceProperties.class);
                    assertThat(properties.enabled()).isFalse();
                    assertThat(properties.topCount()).isEqualTo(10);
                    assertThat(properties.fetchDay()).isEqualTo(DayOfWeek.MONDAY);
                    assertThat(properties.callInterval()).isEqualTo(Duration.ofSeconds(2));
                    assertThat(properties.timeout()).isEqualTo(Duration.ofSeconds(10));
                });
    }

    @Test
    void templateAndUserAgentComeFromTheEnvironmentVariables() {
        runner().withPropertyValues("MEMBERS_URL_TEMPLATE=http://test.invalid/{fundId}?d={date}",
                        "MEMBERS_USER_AGENT=test-agent/9")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    MemberSourceProperties properties = context.getBean(MemberSourceProperties.class);
                    assertThat(properties.enabled()).isTrue();
                    assertThat(properties.userAgent()).isEqualTo("test-agent/9");
                });
    }
}

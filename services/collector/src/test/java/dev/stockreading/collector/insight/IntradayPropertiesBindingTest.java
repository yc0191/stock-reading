package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class IntradayPropertiesBindingTest {

    @EnableConfigurationProperties(IntradayProperties.class)
    static class Wiring {
    }

    @Test
    void isOffByDefaultAndRunsEveryFiveMinutesFromNineToHalfPastThree() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);

        runner.run(context -> {
            assertThat(context).hasNotFailed();
            IntradayProperties properties = context.getBean(IntradayProperties.class);
            assertThat(properties.enabled()).isFalse();
            assertThat(properties.cron()).isEqualTo("0 */5 9-15 * * MON-FRI");
            assertThat(properties.start()).isEqualTo("09:00");
            assertThat(properties.end()).isEqualTo("15:30");
            assertThat(properties.staggerMs()).isEqualTo(15_000);
        });
        runner.withPropertyValues("COLLECTOR_INTRADAY_ENABLED=true", "COLLECTOR_INTRADAY_CRON=0 */10 9-15 * * MON-FRI",
                        "COLLECTOR_INTRADAY_START=09:10", "COLLECTOR_INTRADAY_END=15:20", "COLLECTOR_INTRADAY_STAGGER_MS=0")
                .run(context -> {
                    IntradayProperties properties = context.getBean(IntradayProperties.class);
                    assertThat(properties.enabled()).isTrue();
                    assertThat(properties.cron()).isEqualTo("0 */10 9-15 * * MON-FRI");
                    assertThat(properties.startTime()).hasToString("09:10");
                    assertThat(properties.endTime()).hasToString("15:20");
                    assertThat(properties.staggerMs()).isZero();
                });
    }

    @Test
    void rejectsSettingsThatCannotWork() {
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "15:30", "09:00", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "9시", "15:30", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "09:00", "15:30", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "09:00", "15:30", 60_001))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void blankHoursFallBackToTheTradingHours() {
        IntradayProperties properties = new IntradayProperties(false, "c", "", null, 0);

        assertThat(properties.start()).isEqualTo("09:00");
        assertThat(properties.end()).isEqualTo("15:30");
    }
}

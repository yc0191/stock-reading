package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** 장 운영 상태 조회의 환경변수 이름이 설정에 묶이는지, 값이 없을 때의 기본 동작을 확인한다. */
class MarketStatusPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(MarketStatusProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void withoutValuesNothingIsConfiguredAndTheTimeLimitIsFiveSeconds() {
        runner().run(context -> {
            MarketStatusProperties status = context.getBean(MarketStatusProperties.class);
            assertThat(status.configured()).isFalse();
            assertThat(status.lookupTimeout()).isEqualTo(Duration.ofSeconds(5));
        });
    }

    @Test
    void valuesAreReadFromTheEnvironmentVariableNames() {
        runner().withPropertyValues("BROKER_MARKET_STATUS_PATH=/p", "BROKER_MARKET_STATUS_TODAY_FIELD=a",
                "BROKER_MARKET_STATUS_BUSINESS_DAY_FIELD=b", "BROKER_MARKET_STATUS_TIMEOUT=2s").run(context -> {
                    MarketStatusProperties status = context.getBean(MarketStatusProperties.class);
                    assertThat(status.configured()).isTrue();
                    assertThat(status.path()).isEqualTo("/p");
                    assertThat(status.todayField()).isEqualTo("a");
                    assertThat(status.businessDayField()).isEqualTo("b");
                    assertThat(status.lookupTimeout()).isEqualTo(Duration.ofSeconds(2));
                });
    }
}

package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** 호출 상한의 환경변수 이름이 실제로 설정에 묶이는지, 0 이하이면 기본값으로 대체되는지 확인한다(기본값은 DataSourceWiringTest). */
class BrokerPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(BrokerProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void maxCallsPerSecondIsReadFromTheEnvironmentVariableName() {
        runner().withPropertyValues("BROKER_MAX_CALLS_PER_SECOND=30").run(context ->
                assertThat(context.getBean(BrokerProperties.class).maxCallsPerSecond()).isEqualTo(30));
    }

    @Test
    void zeroOrNegativeMaxCallsPerSecondFallsBackToTheDefault() {
        runner().withPropertyValues("BROKER_MAX_CALLS_PER_SECOND=0").run(context ->
                assertThat(context.getBean(BrokerProperties.class).maxCallsPerSecond()).isEqualTo(15));
        runner().withPropertyValues("BROKER_MAX_CALLS_PER_SECOND=-3").run(context ->
                assertThat(context.getBean(BrokerProperties.class).maxCallsPerSecond()).isEqualTo(15));
    }
}

package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import dev.stockreading.collector.holdings.HoldingsProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 단위 테스트는 클라이언트를 생성자로 직접 만들어 쓰므로, 스프링이 공시 수집 빈을 조립할 수 있는지는 여기서만 보인다.
 * 조립에 실패하면 수집기 전체가 시작하지 못한다.
 */
class EdgarWiringTest {

    @Configuration
    @EnableConfigurationProperties({EdgarProperties.class, HoldingsProperties.class})
    @ComponentScan(basePackages = {"dev.stockreading.collector.edgar", "dev.stockreading.collector.holdings"})
    static class Wiring {

        @Bean
        Clock clock() {
            return Clock.system(ZoneId.of("Asia/Seoul"));
        }

        @Bean
        ObjectMapper mapper() {
            return JsonMapper.builder().build();
        }
    }

    @Test
    void edgarBeansAreAssembledFromApplicationYml() {
        // 셸에 실제 연락처나 보유 종목이 설정돼 있어도 읽지 않게 비운다.
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .withPropertyValues("EDGAR_USER_AGENT=", "ETF_US_HOLDINGS=")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(EdgarPort.class);
                    assertThat(context).hasSingleBean(ExchangePort.class);
                    assertThat(context).hasSingleBean(FilingCollector.class);
                });
    }
}

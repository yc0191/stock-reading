package dev.stockreading.collector;

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

import dev.stockreading.collector.disclosure.DartProperties;
import dev.stockreading.collector.disclosure.DisclosurePort;
import dev.stockreading.collector.insight.InsightProperties;
import dev.stockreading.collector.insight.InsightSourcePort;
import dev.stockreading.collector.marketdata.BrokerProperties;
import dev.stockreading.collector.marketdata.MarketDataPort;
import dev.stockreading.collector.marketdata.MarketStatusProperties;
import dev.stockreading.collector.marketdata.PriceCheckProperties;
import dev.stockreading.collector.news.NewsCollector;
import dev.stockreading.collector.news.NewsSearchPort;
import dev.stockreading.collector.news.NewsSearchProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 단위 테스트로는 보이지 않는 배선을 지킨다: application.yml 과 환경변수로 시세 출처가 실제로 바뀌어야 한다. */
class DataSourceWiringTest {

    @Configuration
    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, MarketStatusProperties.class, NewsSearchProperties.class, DartProperties.class,
            InsightProperties.class, PriceCheckProperties.class})
    @ComponentScan(basePackages = {
            "dev.stockreading.collector.marketdata",
            "dev.stockreading.collector.news",
            "dev.stockreading.collector.disclosure"})
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

    private ApplicationContextRunner runner() {
        // application.yml 을 읽어, 설정 키와 환경변수 연결까지 함께 확인한다.
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void usesTheSimulatorByDefault() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(MarketDataPort.class);
            assertThat(context.getBean(MarketDataPort.class).sourceName()).isEqualTo("simulator");
            assertThat(context).doesNotHaveBean(InsightSourcePort.class);
        });
    }

    @Test
    void switchesToTheBrokerWhenTheEnvironmentVariableSaysSo() {
        runner().withPropertyValues("COLLECTOR_MARKET_DATA_SOURCE=broker").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(MarketDataPort.class);
            assertThat(context.getBean(MarketDataPort.class).sourceName()).isEqualTo("broker");
            assertThat(context).hasSingleBean(InsightSourcePort.class);
            InsightProperties insight = context.getBean(InsightProperties.class);
            assertThat(insight.schedule().enabled()).isFalse();
            assertThat(insight.continents()).containsExactly("C", "S", "E");
            assertThat(insight.dailyMarketClass()).isEqualTo("0");
            assertThat(insight.dailyCount()).isEqualTo(30);
            assertThat(insight.overseasDailyCount()).isEqualTo(30);
            assertThat(insight.overseasMaxTickers()).isEqualTo(100);
        });
    }

    @Test
    void collectionDefaultsAreOneSecondTickThreeSecondAgeAndTenCallsPerSecond() {
        runner().run(context -> {
            assertThat(context.getBean(CollectorProperties.class).tickIntervalMs()).isEqualTo(1_000);
            assertThat(context.getBean(CollectorProperties.class).maxTickAgeMs()).isEqualTo(3_000);
            assertThat(context.getBean(BrokerProperties.class).maxCallsPerSecond()).isEqualTo(10);
        });
    }

    @Test
    void priceCheckIsOnWithPointThreePercentAndConfirmedFieldNames() {
        runner().run(context -> {
            PriceCheckProperties check = context.getBean(PriceCheckProperties.class);
            assertThat(check.enabled()).isTrue();
            assertThat(check.tolerance()).isEqualByComparingTo("0.003");
            // 칸 이름과 부호 값은 실제 응답에서 확인한 값이 기본값이다. 전체 값과 덮어쓰기는 PriceCheckDefaultsTest 가 본다.
            assertThat(check.baseFields()).isNotEmpty();
            assertThat(check.signUp()).isNotEmpty();
            assertThat(check.changeField()).isNotBlank();
            assertThat(check.rateField()).isNotBlank();
            assertThat(check.highField()).isNotBlank();
        });
    }

    @Test
    void newsAndDisclosureBeansAreWiredBehindTheirPorts() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(NewsSearchPort.class);
            assertThat(context).hasSingleBean(DisclosurePort.class);
            assertThat(context).hasSingleBean(NewsCollector.class);
        });
    }

    @Test
    void settingsLeftEmptyDoNotStopTheStartup() {
        // 키와 주소가 비어 있어도 시작은 되고, 호출할 때 명확한 오류로 실패해야 한다.
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(NewsSearchProperties.class).clientId()).isEmpty();
            assertThat(context.getBean(BrokerProperties.class).appKey()).isEmpty();
        });
    }
}

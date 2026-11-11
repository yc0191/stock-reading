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
            // 전일 종가 칸 하나가 하루 안에서 깨져 오는 경우가 있어(2026-11-04 확인) 후보 세 칸의 다수결로 쓴다.
            assertThat(properties.prevCloseFields())
                    .containsExactly("dataBody.bdy_cls_prc", "dataBody.sprc", "dataBody.krx_bdy_clpr");
            assertThat(properties.prevCloseThresholdPercent()).isEqualByComparingTo("3.00");
            assertThat(properties.openPriceFields()).containsExactly("dataBody.opn_prc");
            assertThat(properties.confirmCount()).isEqualTo(3);
            // 첫 체결 전에는 누적 거래량과 시가가 모두 0으로 온다(2026-11-11 확인).
            assertThat(properties.firstTradeFields()).containsExactly("dataBody.acml_vlm", "dataBody.opn_prc");
            assertThat(properties.republishIntervalMinutes()).isEqualTo(30);
            assertThat(properties.republishExceptionPercent()).isEqualByComparingTo("2.00");
        });
    }

    @Test
    void firstTradeFieldsAreReadFromTheEnvironmentVariableNameAndCanBeEmptied() {
        runner().withPropertyValues("MARKET_FIRST_TRADE_FIELDS=other.a,other.b").run(context ->
                assertThat(context.getBean(MarketProperties.class).firstTradeFields()).containsExactly("other.a", "other.b"));
        runner().withPropertyValues("MARKET_FIRST_TRADE_FIELDS=").run(context ->
                assertThat(context.getBean(MarketProperties.class).firstTradeFields()).isEmpty());
    }

    @Test
    void prevCloseFieldsAreReadFromTheEnvironmentVariableName() {
        runner().withPropertyValues("MARKET_PREV_CLOSE_FIELDS=other.a,other.b").run(context ->
                assertThat(context.getBean(MarketProperties.class).prevCloseFields()).containsExactly("other.a", "other.b"));
    }

    @Test
    void openPriceFieldsAreReadFromTheEnvironmentVariableName() {
        runner().withPropertyValues("MARKET_OPEN_PRICE_FIELDS=other.open").run(context ->
                assertThat(context.getBean(MarketProperties.class).openPriceFields()).containsExactly("other.open"));
    }
}

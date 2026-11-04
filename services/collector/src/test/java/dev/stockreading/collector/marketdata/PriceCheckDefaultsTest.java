package dev.stockreading.collector.marketdata;

import static dev.stockreading.collector.marketdata.PriceFieldCheck.Check.CHANGE;
import static dev.stockreading.collector.marketdata.PriceFieldCheck.Check.HIGH_LOW;
import static dev.stockreading.collector.marketdata.PriceFieldCheck.Check.RATE;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 의 칸 이름 기본값만으로 검사가 실제로 켜지는지, 환경변수로 덮어쓰거나 비우면 따라 바뀌는지 본다.
 * 값은 모두 가짜다. 전일 종가 10000, 하락 100(-1.00%)이면 맞는 현재가는 9900 이다.
 */
class PriceCheckDefaultsTest {

    @Configuration
    @EnableConfigurationProperties(PriceCheckProperties.class)
    static class Wiring {
    }

    private static final String[] ALL_FIELD_VARIABLES = {
            "BROKER_PRICE_CHECK_BASE_FIELDS", "BROKER_PRICE_CHECK_CHANGE_FIELD", "BROKER_PRICE_CHECK_CHANGE_SIGN_FIELD",
            "BROKER_PRICE_CHECK_SIGN_UP", "BROKER_PRICE_CHECK_SIGN_FLAT", "BROKER_PRICE_CHECK_SIGN_DOWN",
            "BROKER_PRICE_CHECK_RATE_FIELD", "BROKER_PRICE_CHECK_HIGH_FIELD", "BROKER_PRICE_CHECK_LOW_FIELD"};

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    private static PriceFieldCheck check(PriceCheckProperties properties) {
        return new PriceFieldCheck(properties, JsonMapper.builder().build());
    }

    // 2026-11-04 실제 응답의 칸 이름으로 만든 가짜 본문. 전일 종가 칸 셋 중 하나만 넣어도 검사는 판단한다.
    private static String defaultNamedBody(String price) {
        return "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{\"now_prc\":\"" + price + "\""
                + ",\"bdy_cls_prc\":\"10000\",\"bdy_cmpr\":\"100\",\"bdy_cmpr_ccd\":\"5\",\"up_dwn_r_p2\":\"   -1.00\""
                + ",\"hgh_prc\":\"10100\",\"lw_prc\":\"9800\"}}";
    }

    @Test
    void defaultsAreTheFieldNamesConfirmedOnRealResponses() {
        runner().run(context -> {
            PriceCheckProperties properties = context.getBean(PriceCheckProperties.class);
            assertThat(properties.baseFields()).containsExactly("bdy_cls_prc", "sprc", "krx_bdy_clpr");
            assertThat(properties.changeField()).isEqualTo("bdy_cmpr");
            assertThat(properties.changeSignField()).isEqualTo("bdy_cmpr_ccd");
            assertThat(properties.signUp()).containsExactly("1", "2");
            assertThat(properties.signFlat()).containsExactly("3");
            assertThat(properties.signDown()).containsExactly("4", "5");
            assertThat(properties.rateField()).isEqualTo("up_dwn_r_p2");
            assertThat(properties.highField()).isEqualTo("hgh_prc");
            assertThat(properties.lowField()).isEqualTo("lw_prc");
            assertThat(properties.noFieldsConfigured()).isFalse();
        });
    }

    @Test
    void defaultsAloneTurnTheCheckOn() {
        runner().run(context -> {
            PriceFieldCheck check = check(context.getBean(PriceCheckProperties.class));

            assertThat(check.failedChecks(9900, defaultNamedBody("9900"))).isEmpty();
            assertThat(check.failedChecks(12345, defaultNamedBody("12345"))).containsExactly(CHANGE, RATE, HIGH_LOW);
        });
    }

    @Test
    void environmentVariablesReplaceTheDefaults() {
        runner().withPropertyValues("BROKER_PRICE_CHECK_BASE_FIELDS=" + PriceChecks.BASE,
                        "BROKER_PRICE_CHECK_CHANGE_FIELD=" + PriceChecks.CHANGE,
                        "BROKER_PRICE_CHECK_CHANGE_SIGN_FIELD=" + PriceChecks.SIGN,
                        "BROKER_PRICE_CHECK_SIGN_UP=U", "BROKER_PRICE_CHECK_SIGN_FLAT=F", "BROKER_PRICE_CHECK_SIGN_DOWN=D",
                        "BROKER_PRICE_CHECK_RATE_FIELD=" + PriceChecks.RATE,
                        "BROKER_PRICE_CHECK_HIGH_FIELD=" + PriceChecks.HIGH,
                        "BROKER_PRICE_CHECK_LOW_FIELD=" + PriceChecks.LOW)
                .run(context -> {
                    PriceCheckProperties properties = context.getBean(PriceCheckProperties.class);
                    assertThat(properties.baseFields()).containsExactly(PriceChecks.BASE);
                    assertThat(properties.signDown()).containsExactly("D");
                    PriceFieldCheck check = check(properties);
                    String fakeNamed = PriceChecks.body("12345", "10000", "100", "D", "-1.00", "10100", "9800");

                    assertThat(check.failedChecks(12345, fakeNamed)).containsExactly(CHANGE, RATE, HIGH_LOW);
                    // 기본 칸 이름은 더 이상 읽지 않으므로, 기본 이름만 있는 본문은 판단할 검사가 없어 통과한다.
                    assertThat(check.failedChecks(12345, defaultNamedBody("12345"))).isEmpty();
                });
    }

    @Test
    void emptyEnvironmentVariablesTurnEveryCheckOffAsBefore() {
        String[] empty = new String[ALL_FIELD_VARIABLES.length];
        for (int i = 0; i < empty.length; i++) {
            empty[i] = ALL_FIELD_VARIABLES[i] + "=";
        }
        runner().withPropertyValues(empty).run(context -> {
            PriceCheckProperties properties = context.getBean(PriceCheckProperties.class);
            assertThat(properties.enabled()).isTrue();
            assertThat(properties.noFieldsConfigured()).isTrue();

            assertThat(check(properties).failedChecks(12345, defaultNamedBody("12345"))).isEmpty();
        });
    }
}

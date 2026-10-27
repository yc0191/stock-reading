package dev.stockreading.market;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

// 설정 기본값이 문서와 같은지, 환경변수 이름이 실제로 적용되는지를 application.yml 그대로 읽어 확인한다.
class MarketPropertiesDefaultsTest {

    private MarketProperties bind(Map<String, Object> overrides) throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        if (!overrides.isEmpty()) {
            environment.getPropertySources().addFirst(new MapPropertySource("overrides", overrides));
        }
        for (PropertySource<?> source : new YamlPropertySourceLoader()
                .load("application", new ClassPathResource("application.yml"))) {
            environment.getPropertySources().addLast(source);
        }
        return Binder.get(environment).bind("market", MarketProperties.class).get();
    }

    @Test
    void defaultsMatchTheDocumentedValues() throws IOException {
        MarketProperties properties = bind(Map.of());

        assertThat(properties.windowMinutes()).isEqualTo(5);
        assertThat(properties.windowThresholdPercent()).isEqualByComparingTo("1.00");
        assertThat(properties.prevCloseField()).isEqualTo("bdy_cls_prc");
        assertThat(properties.prevCloseThresholdPercent()).isEqualByComparingTo("3.00");
    }

    @Test
    void prevCloseFieldCanBeChangedWithTheDocumentedEnvironmentVariable() throws IOException {
        MarketProperties properties = bind(Map.of("MARKET_PREV_CLOSE_FIELD", "other.field"));

        assertThat(properties.prevCloseField()).isEqualTo("other.field");
    }
}

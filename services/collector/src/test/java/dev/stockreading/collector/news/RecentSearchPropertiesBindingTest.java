package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** application.yml 의 최근 검색 다시 쓰기 설정이 실제로 묶이는지, 잘못된 값이 시작 단계에서 막히는지 확인한다. */
class RecentSearchPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(RecentSearchProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void defaultsReuseForFifteenMinutesAndWaitLongerThanOneSearch() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            RecentSearchProperties properties = context.getBean(RecentSearchProperties.class);
            assertThat(properties.window()).isEqualTo(Duration.ofMinutes(15));
            assertThat(properties.waitLimit()).isEqualTo(Duration.ofSeconds(25));
            assertThat(properties.maxEntries()).isEqualTo(2000);
        });
    }

    @Test
    void reuseWindowMatchesTheRegularNewsCollectionInterval() {
        runner().run(context -> assertThat(context.getBean(RecentSearchProperties.class).window())
                .isEqualTo(Duration.ofMillis(context.getEnvironment()
                        .getRequiredProperty("collector.schedule.news-interval-ms", Long.class))));
    }

    @Test
    void invalidValuesFailStartup() {
        runner().withPropertyValues("collector.news-reuse.window=0s").run(context -> assertThat(context).hasFailed());
        runner().withPropertyValues("collector.news-reuse.wait-limit=-1s").run(context -> assertThat(context).hasFailed());
        runner().withPropertyValues("collector.news-reuse.max-entries=0").run(context -> assertThat(context).hasFailed());
    }
}

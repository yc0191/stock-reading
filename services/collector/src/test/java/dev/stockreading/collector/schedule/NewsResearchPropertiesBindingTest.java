package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** 재검색 검색어 상한이 application.yml 과 환경변수로 묶이는지, 잘못된 값이 시작 단계에서 막히는지 확인한다. */
class NewsResearchPropertiesBindingTest {

    @Configuration
    @EnableConfigurationProperties(NewsResearchProperties.class)
    static class Wiring {
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);
    }

    @Test
    void researchQueryCapDefaultsToThreeWhenTheEnvironmentVariableIsAbsent() {
        runner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(NewsResearchProperties.class).maxQueries()).isEqualTo(3);
        });
    }

    @Test
    void researchQueryCapIsReadFromTheEnvironmentVariableName() {
        runner().withPropertyValues("NEWS_RESEARCH_MAX_QUERIES=1").run(context ->
                assertThat(context.getBean(NewsResearchProperties.class).maxQueries()).isEqualTo(1));
    }

    @Test
    void zeroOrNegativeResearchQueryCapFailsStartup() {
        runner().withPropertyValues("NEWS_RESEARCH_MAX_QUERIES=0").run(context -> assertThat(context).hasFailed());
        runner().withPropertyValues("NEWS_RESEARCH_MAX_QUERIES=-1").run(context -> assertThat(context).hasFailed());
    }
}

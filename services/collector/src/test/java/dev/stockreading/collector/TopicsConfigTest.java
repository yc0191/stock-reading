package dev.stockreading.collector;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

/** 단위 테스트로는 보이지 않는 배선을 지킨다: 설정이 스프링 컨텍스트로 로드되어야 한다. */
class TopicsConfigTest {

    @Test
    void topicsConfigurationLoadsWithoutBeanNameClash() {
        new ApplicationContextRunner()
                .withUserConfiguration(Topics.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(NewTopics.class);
                });
    }
}

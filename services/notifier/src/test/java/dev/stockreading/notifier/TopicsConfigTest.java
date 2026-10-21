package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** 소비자는 생산자와 같은 파티션 수로 토픽을 선언한다(시작 순서와 무관하게 안전하도록). */
class TopicsConfigTest {

    @Test
    void declaresBriefingGeneratedWithThreePartitions() {
        new ApplicationContextRunner()
                .withUserConfiguration(Topics.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    NewTopic topic = context.getBean(NewTopic.class);
                    assertThat(topic.name()).isEqualTo("briefing.generated");
                    assertThat(topic.numPartitions()).isEqualTo(3);
                });
    }
}

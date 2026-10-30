package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.Collectors;

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
                    assertThat(declared(context.getBeansOfType(NewTopic.class)))
                            .containsEntry("briefing.generated", 3);
                });
    }

    @Test
    void declaresDailyBriefingGeneratedWithOnePartitionLikeTheBriefingService() {
        new ApplicationContextRunner()
                .withUserConfiguration(Topics.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    // 브리핑 main.py 의 TOPIC_PARTITIONS 와 같아야 한다.
                    assertThat(declared(context.getBeansOfType(NewTopic.class)))
                            .containsExactlyInAnyOrderEntriesOf(Map.of(
                                    "briefing.generated", 3,
                                    "briefing.daily.generated", 1));
                });
    }

    private static Map<String, Integer> declared(Map<String, NewTopic> beans) {
        return beans.values().stream().collect(Collectors.toMap(NewTopic::name, NewTopic::numPartitions));
    }
}

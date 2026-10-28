package dev.stockreading.collector;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
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

    @Test
    void declaresEveryTopicWithTheAgreedPartitionCount() {
        new ApplicationContextRunner()
                .withUserConfiguration(Topics.class)
                .run(context -> {
                    Map<String, Integer> partitions = new HashMap<>();
                    // getNewTopics 는 패키지 공개라서 리플렉션으로 읽는다.
                    Method getter = NewTopics.class.getDeclaredMethod("getNewTopics");
                    getter.setAccessible(true);
                    for (Object topic : (Collection<?>) getter.invoke(context.getBean(NewTopics.class))) {
                        NewTopic newTopic = (NewTopic) topic;
                        partitions.put(newTopic.name(), newTopic.numPartitions());
                    }

                    assertThat(partitions)
                            .containsEntry(Topics.SCHEDULE_TICK, 1)
                            .containsEntry(Topics.MARKET_SNAPSHOT, 3)
                            .containsEntry(Topics.MARKET_SNAPSHOT_FAILED, 3)
                            .containsEntry(Topics.NEWS_COLLECTED, 3)
                            .containsEntry(Topics.MARKET_INSIGHT, 3);
                    assertThat(Topics.MARKET_SNAPSHOT_FAILED).isEqualTo("market.snapshot.failed");
                    assertThat(Topics.NEWS_COLLECTED).isEqualTo("news.collected");
                    assertThat(Topics.MARKET_INSIGHT).isEqualTo("market.insight");
                });
    }
}

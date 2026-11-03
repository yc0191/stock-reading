package dev.stockreading.market;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** 토픽은 생산자와 소비자가 합의한 파티션 수로 선언되어야 한다(시작 순서와 무관하게 안전하도록). */
class TopicsConfigTest {

    @Test
    void declaresSnapshotMoveInsightHistoryAndContextTopicsWithThreePartitions() {
        new ApplicationContextRunner()
                .withUserConfiguration(Topics.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Map<String, NewTopic> topics = context.getBeansOfType(NewTopic.class);
                    assertThat(topics.values()).extracting(NewTopic::name)
                            .containsExactlyInAnyOrder("market.snapshot", "market.snapshot.failed", "market.move",
                                    "market.insight", "market.history.requested", "market.history.responded",
                                    "market.context.requested", "market.context.responded");
                    assertThat(topics.values()).extracting(NewTopic::numPartitions).containsOnly(3);
                });
    }
}

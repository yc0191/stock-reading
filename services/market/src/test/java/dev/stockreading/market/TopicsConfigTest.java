package dev.stockreading.market;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** 토픽은 생산자와 소비자가 합의한 파티션 수로 선언되어야 한다(시작 순서와 무관하게 안전하도록). */
class TopicsConfigTest {

    @Test
    void declaresTheTopicsWithAgreedPartitionsAndKeepsOnlyTheLastHoldingsList() {
        new ApplicationContextRunner()
                .withUserConfiguration(Topics.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Map<String, NewTopic> topics = context.getBeansOfType(NewTopic.class);
                    assertThat(topics.values()).extracting(NewTopic::name)
                            .containsExactlyInAnyOrder("market.snapshot", "market.snapshot.failed", "market.move",
                                    "market.insight", "market.history.requested", "market.history.responded",
                                    "market.context.requested", "market.context.responded",
                                    "market.daily.requested", "market.daily.responded", "market.holdings", "schedule.tick");
                    // 보유 종목 목록과 tick만 파티션이 하나이고, 소비자·생산자(수집기, 뉴스)도 같은 값으로 선언해야 한다.
                    assertThat(topics.values().stream()
                            .filter(t -> !t.name().equals("market.holdings") && !t.name().equals("schedule.tick")))
                            .extracting(NewTopic::numPartitions).containsOnly(3);
                    assertThat(topics.get("scheduleTickTopic").numPartitions()).isEqualTo(1);
                    NewTopic holdings = topics.get("marketHoldingsTopic");
                    assertThat(holdings.numPartitions()).isEqualTo(1);
                    assertThat(holdings.configs()).containsEntry("cleanup.policy", "compact");
                });
    }
}

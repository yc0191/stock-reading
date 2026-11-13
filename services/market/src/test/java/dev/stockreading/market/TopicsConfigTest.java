package dev.stockreading.market;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/** 토픽은 생산자와 소비자가 합의한 파티션 수로 선언되어야 한다(시작 순서와 무관하게 안전하도록). */
class TopicsConfigTest {

    @Test
    void declaresTheTopicsWithAgreedPartitions() {
        new ApplicationContextRunner()
                .withUserConfiguration(Topics.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    Map<String, NewTopic> topics = context.getBeansOfType(NewTopic.class);
                    assertThat(topics.values()).extracting(NewTopic::name)
                            .containsExactlyInAnyOrder("market.snapshot", "market.snapshot.failed", "market.move",
                                    "market.insight", "etf.members.fetched");
                    // 브리핑의 시세 조회는 내부 조회 API 라 요청·응답 토픽을 선언하지 않는다.
                    assertThat(topics.values()).extracting(NewTopic::name)
                            .noneMatch(name -> name.endsWith(".requested") || name.endsWith(".responded"));
                    // 생산자(수집기)도 같은 값으로 선언해야 한다.
                    assertThat(topics.values()).extracting(NewTopic::numPartitions).containsOnly(3);
                });
    }
}

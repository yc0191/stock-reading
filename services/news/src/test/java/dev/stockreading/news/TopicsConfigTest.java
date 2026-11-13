package dev.stockreading.news;

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
                            .containsExactlyInAnyOrder("news.collected", "news.articles.requested",
                                    "news.articles.responded", "filing.collected", "news.filings.requested",
                                    "news.filings.responded", "collection.completed", "news.stored");
                    // 수집 완료와 저장 끝은 collector 의 collection.completed 와 같은 1개, 나머지는 3개다.
                    assertThat(topics.values()).filteredOn(t -> t.name().equals("collection.completed")
                            || t.name().equals("news.stored")).extracting(NewTopic::numPartitions).containsOnly(1);
                    assertThat(topics.values()).filteredOn(t -> !t.name().equals("collection.completed")
                            && !t.name().equals("news.stored")).extracting(NewTopic::numPartitions).containsOnly(3);
                });
    }
}

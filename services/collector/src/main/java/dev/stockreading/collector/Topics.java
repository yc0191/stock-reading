package dev.stockreading.collector;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

@Configuration
public class Topics {

    public static final String SCHEDULE_TICK = "schedule.tick";
    public static final String MARKET_SNAPSHOT = "market.snapshot";
    public static final String MARKET_SNAPSHOT_FAILED = "market.snapshot.failed";

    public static final String NEWS_COLLECTED = "news.collected";
    public static final String MARKET_INSIGHT = "market.insight";
    public static final String FILING_COLLECTED = "filing.collected";

    // 파티션 수는 시작값이며, ADR-005 부하 검증으로 확인한다.
    // market.snapshot.failed 는 market.snapshot 과 같은 키(종목)와 파티션 수 3으로 맞춘다. 같은 종목의
    // 성공과 실패가 같은 파티션 순서로 흐르고, 소비 쪽(market) 선언과도 같아야 한다.
    // news.collected, market.insight 의 파티션 수는 소비 쪽 선언과 같아야 한다(ADR-013, ADR-018).
    // filing.collected 도 같은 이유로 3이며, 소비 쪽(news) 선언과 같아야 한다.
    @Bean
    NewTopics kafkaTopics() {
        return new NewTopics(
                TopicBuilder.name(SCHEDULE_TICK).partitions(1).replicas(1).build(),
                TopicBuilder.name(MARKET_SNAPSHOT).partitions(3).replicas(1).build(),
                TopicBuilder.name(MARKET_SNAPSHOT_FAILED).partitions(3).replicas(1).build(),
                TopicBuilder.name(NEWS_COLLECTED).partitions(3).replicas(1).build(),
                TopicBuilder.name(MARKET_INSIGHT).partitions(3).replicas(1).build(),
                TopicBuilder.name(FILING_COLLECTED).partitions(3).replicas(1).build());
    }
}

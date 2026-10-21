package dev.stockreading.collector;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;

@Configuration
public class Topics {

    public static final String SCHEDULE_TICK = "schedule.tick";
    public static final String MARKET_SNAPSHOT = "market.snapshot";

    // 파티션 수는 시작값이며, ADR-005 부하 검증으로 확인한다.
    @Bean
    NewTopics kafkaTopics() {
        return new NewTopics(
                TopicBuilder.name(SCHEDULE_TICK).partitions(1).replicas(1).build(),
                TopicBuilder.name(MARKET_SNAPSHOT).partitions(3).replicas(1).build());
    }
}

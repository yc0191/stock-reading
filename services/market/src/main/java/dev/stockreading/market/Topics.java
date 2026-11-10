package dev.stockreading.market;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 이 서비스가 소비하거나 생산하는 모든 토픽을 선언해서 시작 순서와 무관하게 동작하게 한다
 * (브로커 자동 생성은 꺼져 있다). 같은 토픽의 생산자와 소비자는 파티션 수가 같아야 한다.
 */
@Configuration
public class Topics {

    public static final String MARKET_SNAPSHOT = "market.snapshot";
    public static final String MARKET_SNAPSHOT_FAILED = "market.snapshot.failed";
    public static final String MARKET_MOVE = "market.move";
    public static final String MARKET_INSIGHT = "market.insight";
    public static final String MARKET_HISTORY_REQUESTED = "market.history.requested";
    public static final String MARKET_HISTORY_RESPONDED = "market.history.responded";
    public static final String MARKET_CONTEXT_REQUESTED = "market.context.requested";
    public static final String MARKET_CONTEXT_RESPONDED = "market.context.responded";
    public static final String MARKET_DAILY_REQUESTED = "market.daily.requested";
    public static final String MARKET_DAILY_RESPONDED = "market.daily.responded";
    public static final String MARKET_HOLDINGS = "market.holdings";
    public static final String SCHEDULE_TICK = "schedule.tick";
    public static final int PARTITIONS = 3;
    // 보유 종목 목록은 키가 하나인 압축 토픽이라 파티션 하나로 충분하고, 소비자(수집기, 뉴스)의 선언과 같아야 한다.
    public static final int HOLDINGS_PARTITIONS = 1;
    // 수집기의 선언과 같아야 한다. 파티션이 1개라서 이 토픽을 받는 리스너는 서로 다른 consumer group을 쓴다.
    public static final int SCHEDULE_TICK_PARTITIONS = 1;

    @Bean
    NewTopic marketSnapshotTopic() {
        return TopicBuilder.name(MARKET_SNAPSHOT).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketSnapshotFailedTopic() {
        return TopicBuilder.name(MARKET_SNAPSHOT_FAILED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketMoveTopic() {
        return TopicBuilder.name(MARKET_MOVE).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketInsightTopic() {
        return TopicBuilder.name(MARKET_INSIGHT).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketHistoryRequestedTopic() {
        return TopicBuilder.name(MARKET_HISTORY_REQUESTED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketHistoryRespondedTopic() {
        return TopicBuilder.name(MARKET_HISTORY_RESPONDED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketContextRequestedTopic() {
        return TopicBuilder.name(MARKET_CONTEXT_REQUESTED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketContextRespondedTopic() {
        return TopicBuilder.name(MARKET_CONTEXT_RESPONDED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketDailyRequestedTopic() {
        return TopicBuilder.name(MARKET_DAILY_REQUESTED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic marketDailyRespondedTopic() {
        return TopicBuilder.name(MARKET_DAILY_RESPONDED).partitions(PARTITIONS).replicas(1).build();
    }

    // 키가 고정이고 마지막 목록만 남는 압축 토픽이다. 새로 시작한 소비자가 마지막 목록을 받는다.
    @Bean
    NewTopic marketHoldingsTopic() {
        return TopicBuilder.name(MARKET_HOLDINGS).partitions(HOLDINGS_PARTITIONS).replicas(1)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT).build();
    }

    @Bean
    NewTopic scheduleTickTopic() {
        return TopicBuilder.name(SCHEDULE_TICK).partitions(SCHEDULE_TICK_PARTITIONS).replicas(1).build();
    }
}

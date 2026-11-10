package dev.stockreading.collector;

import org.apache.kafka.common.config.TopicConfig;
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
    public static final String COLLECTION_COMPLETED = "collection.completed";
    public static final String FILING_COLLECTED = "filing.collected";
    public static final String NEWS_RESEARCH_REQUESTED = "news.research.requested";
    public static final String MARKET_HOLDINGS = "market.holdings";
    public static final String NEWS_SEARCH_QUERY_LIST = "news.search-queries";

    // 파티션 수는 부하를 재서 정한 값이 아니라 시작값이다.
    // market.snapshot.failed 는 market.snapshot 과 같은 키(종목)와 파티션 수 3으로 맞춘다. 같은 종목의
    // 성공과 실패가 같은 파티션 순서로 흐르고, 소비 쪽(market) 선언과도 같아야 한다.
    // news.collected, market.insight, filing.collected 의 파티션 수는 소비 쪽 선언과 같아야 한다.
    // collection.completed 는 키가 jobType 뿐이라 파티션 1개이며, 소비 쪽(briefing) 선언과 같아야 한다.
    // news.research.requested 는 생산 쪽(briefing) 선언과 같아야 한다.
    // market.holdings 와 news.search-queries 는 키가 하나인 압축 토픽(마지막 목록만 남음)이고 파티션 1개이며,
    // 생산 쪽(market, news) 선언과 같아야 한다.
    @Bean
    NewTopics kafkaTopics() {
        return new NewTopics(
                TopicBuilder.name(SCHEDULE_TICK).partitions(1).replicas(1).build(),
                TopicBuilder.name(MARKET_SNAPSHOT).partitions(3).replicas(1).build(),
                TopicBuilder.name(MARKET_SNAPSHOT_FAILED).partitions(3).replicas(1).build(),
                TopicBuilder.name(NEWS_COLLECTED).partitions(3).replicas(1).build(),
                TopicBuilder.name(MARKET_INSIGHT).partitions(3).replicas(1).build(),
                TopicBuilder.name(COLLECTION_COMPLETED).partitions(1).replicas(1).build(),
                TopicBuilder.name(FILING_COLLECTED).partitions(3).replicas(1).build(),
                TopicBuilder.name(NEWS_RESEARCH_REQUESTED).partitions(3).replicas(1).build(),
                TopicBuilder.name(MARKET_HOLDINGS).partitions(1).replicas(1)
                        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT).build(),
                TopicBuilder.name(NEWS_SEARCH_QUERY_LIST).partitions(1).replicas(1)
                        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT).build());
    }
}

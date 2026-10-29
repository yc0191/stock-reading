package dev.stockreading.news;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * 이 서비스가 소비하거나 생산하는 모든 토픽을 선언해서 시작 순서와 무관하게 동작하게 한다
 * (브로커 자동 생성은 꺼져 있다). 같은 토픽의 생산자와 소비자는 파티션 수가 같아야 한다.
 */
@Configuration
public class Topics {

    public static final String NEWS_COLLECTED = "news.collected";
    public static final String NEWS_ARTICLES_REQUESTED = "news.articles.requested";
    public static final String NEWS_ARTICLES_RESPONDED = "news.articles.responded";
    public static final String FILING_COLLECTED = "filing.collected";
    public static final int PARTITIONS = 3;

    @Bean
    NewTopic newsCollectedTopic() {
        return TopicBuilder.name(NEWS_COLLECTED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic newsArticlesRequestedTopic() {
        return TopicBuilder.name(NEWS_ARTICLES_REQUESTED).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic newsArticlesRespondedTopic() {
        return TopicBuilder.name(NEWS_ARTICLES_RESPONDED).partitions(PARTITIONS).replicas(1).build();
    }

    // 생산자(collector)와 같은 파티션 수여야 한다.
    @Bean
    NewTopic filingCollectedTopic() {
        return TopicBuilder.name(FILING_COLLECTED).partitions(PARTITIONS).replicas(1).build();
    }
}

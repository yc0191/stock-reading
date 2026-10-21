package dev.stockreading.notifier;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** 이 서비스가 소비하는 토픽을 선언해서 시작 순서와 무관하게 동작하게 한다(브로커 자동 생성은 꺼져 있다). */
@Configuration
public class Topics {

    public static final String BRIEFING_GENERATED = "briefing.generated";
    public static final int PARTITIONS = 3; // 생산자(브리핑 서비스)와 같아야 한다

    @Bean
    NewTopic briefingGeneratedTopic() {
        return TopicBuilder.name(BRIEFING_GENERATED).partitions(PARTITIONS).replicas(1).build();
    }
}

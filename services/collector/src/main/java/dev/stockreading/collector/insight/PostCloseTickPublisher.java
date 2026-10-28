package dev.stockreading.collector.insight;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.event.TickPayload;
import tools.jackson.databind.ObjectMapper;

/**
 * 마감 후 작업의 schedule.tick 만 발행한다. 휴장일은 판정하지 않으므로 휴장일에도 발행된다.
 * collector.insight.schedule.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.insight.schedule.enabled", havingValue = "true")
class PostCloseTickPublisher {

    static final String JOB_COLLECT_POST_CLOSE = "collect-post-close";
    private static final Logger log = LoggerFactory.getLogger(PostCloseTickPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    PostCloseTickPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Scheduled(cron = "${collector.insight.schedule.cron}", zone = "Asia/Seoul")
    void publishPostCloseTick() {
        OffsetDateTime scheduledFor = OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        String scheduledForText = EventFactory.format(scheduledFor);
        Envelope<TickPayload> tick = EventFactory.create(
                clock,
                "schedule.tick",
                scheduledFor,
                JOB_COLLECT_POST_CLOSE + "-" + scheduledForText,
                new TickPayload(JOB_COLLECT_POST_CLOSE, scheduledForText));
        kafka.send(Topics.SCHEDULE_TICK, JOB_COLLECT_POST_CLOSE, mapper.writeValueAsString(tick));
        log.info("published schedule.tick jobType={} scheduledFor={}", JOB_COLLECT_POST_CLOSE, scheduledForText);
    }
}

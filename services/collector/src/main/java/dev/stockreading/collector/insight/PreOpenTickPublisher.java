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
 * 장 시작 전 작업의 schedule.tick 만 발행한다. 휴장일은 판정하지 않으므로 휴장일에도 발행된다.
 * 이 tick 은 해외 일봉·세계지수(InsightTickListener)와 SEC 공시(FilingTickListener)가 함께 받는다.
 * collector.pre-open.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.pre-open.enabled", havingValue = "true")
public class PreOpenTickPublisher {

    // 공시 리스너(schedule 패키지)도 이 값으로 tick 을 고르므로 공개한다.
    public static final String JOB_COLLECT_PRE_OPEN = "collect-pre-open";
    private static final Logger log = LoggerFactory.getLogger(PreOpenTickPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    PreOpenTickPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Scheduled(cron = "${collector.pre-open.cron}", zone = "Asia/Seoul")
    void publishPreOpenTick() {
        OffsetDateTime scheduledFor = OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        String scheduledForText = EventFactory.format(scheduledFor);
        Envelope<TickPayload> tick = EventFactory.create(
                clock,
                "schedule.tick",
                scheduledFor,
                JOB_COLLECT_PRE_OPEN + "-" + scheduledForText,
                new TickPayload(JOB_COLLECT_PRE_OPEN, scheduledForText));
        kafka.send(Topics.SCHEDULE_TICK, JOB_COLLECT_PRE_OPEN, mapper.writeValueAsString(tick));
        log.info("published schedule.tick jobType={} scheduledFor={}", JOB_COLLECT_PRE_OPEN, scheduledForText);
    }
}

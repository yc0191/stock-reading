package dev.stockreading.collector.schedule;

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
 * 데일리 브리핑(아침 판, 마감 후 판)의 시작 신호인 schedule.tick 만 발행한다. 자료는 모으지 않는다.
 * 휴장일은 판정하지 않으므로 휴장일에도 발행된다.
 * collector.daily-briefing.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.daily-briefing.enabled", havingValue = "true")
class DailyBriefingTickPublisher {

    static final String JOB_DAILY_BRIEFING_MORNING = "daily-briefing-morning";
    static final String JOB_DAILY_BRIEFING_POST_CLOSE = "daily-briefing-post-close";
    private static final Logger log = LoggerFactory.getLogger(DailyBriefingTickPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    DailyBriefingTickPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Scheduled(cron = "${collector.daily-briefing.morning-cron}", zone = "Asia/Seoul")
    void publishMorningTick() {
        publishTick(JOB_DAILY_BRIEFING_MORNING);
    }

    @Scheduled(cron = "${collector.daily-briefing.post-close-cron}", zone = "Asia/Seoul")
    void publishPostCloseTick() {
        publishTick(JOB_DAILY_BRIEFING_POST_CLOSE);
    }

    private void publishTick(String jobType) {
        OffsetDateTime scheduledFor = OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        String scheduledForText = EventFactory.format(scheduledFor);
        Envelope<TickPayload> tick = EventFactory.create(
                clock,
                "schedule.tick",
                scheduledFor,
                jobType + "-" + scheduledForText,
                new TickPayload(jobType, scheduledForText));
        kafka.send(Topics.SCHEDULE_TICK, jobType, mapper.writeValueAsString(tick));
        log.info("published schedule.tick jobType={} scheduledFor={}", jobType, scheduledForText);
    }
}

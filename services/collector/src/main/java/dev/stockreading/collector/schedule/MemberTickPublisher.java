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
 * ETF 구성 종목 조회 tick(collect-members)만 발행한다. 이 tick은 시세처리기(market)가 받아 그날 실제로 받을 차례인
 * ETF(평소엔 주 1회, 지수 교체 달엔 매일)의 구성만 받으므로 여기서는 평일 하루 한 번만 발행한다.
 * 휴장일은 판정하지 않는다. collector.members.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.members.enabled", havingValue = "true")
class MemberTickPublisher {

    static final String JOB_COLLECT_MEMBERS = "collect-members";
    private static final Logger log = LoggerFactory.getLogger(MemberTickPublisher.class);

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    MemberTickPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Scheduled(cron = "${collector.members.cron}", zone = "Asia/Seoul")
    void publishMembersTick() {
        OffsetDateTime scheduledFor = OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        String scheduledForText = EventFactory.format(scheduledFor);
        Envelope<TickPayload> tick = EventFactory.create(
                clock,
                "schedule.tick",
                scheduledFor,
                JOB_COLLECT_MEMBERS + "-" + scheduledForText,
                new TickPayload(JOB_COLLECT_MEMBERS, scheduledForText));
        kafka.send(Topics.SCHEDULE_TICK, JOB_COLLECT_MEMBERS, mapper.writeValueAsString(tick));
        log.info("published schedule.tick jobType={} scheduledFor={}", JOB_COLLECT_MEMBERS, scheduledForText);
    }
}

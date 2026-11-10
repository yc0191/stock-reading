package dev.stockreading.collector.insight;

import java.time.Clock;
import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
import dev.stockreading.collector.marketdata.MarketDayPort;
import tools.jackson.databind.ObjectMapper;

/**
 * 장중 작업의 schedule.tick 만 발행한다. 휴장일(시세 제공자의 장 운영 상태로 판정, 판정하지 못하면 영업일)에는 발행하지 않는다.
 * cron 이 주기를 정하고, 평일과 시작·끝 시각(한국 시간) 밖이면 발행하지 않는다.
 * collector.intraday.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.intraday.enabled", havingValue = "true")
class IntradayTickPublisher {

    static final String JOB_COLLECT_INTRADAY = "collect-intraday";
    private static final ZoneId KOREA = ZoneId.of("Asia/Seoul");
    private static final Logger log = LoggerFactory.getLogger(IntradayTickPublisher.class);

    private final IntradayProperties properties;
    private final MarketDayPort marketDay;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    IntradayTickPublisher(IntradayProperties properties, MarketDayPort marketDay, KafkaTemplate<String, String> kafka,
                          ObjectMapper mapper, Clock clock) {
        this.properties = properties;
        this.marketDay = marketDay;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Scheduled(cron = "${collector.intraday.cron}", zone = "Asia/Seoul")
    void publishIntradayTick() {
        OffsetDateTime scheduledFor = OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
        if (!withinHours(scheduledFor)) {
            log.debug("intraday tick skipped outside hours scheduledFor={}", scheduledFor);
            return;
        }
        if (!marketDay.isBusinessDay()) {
            log.info("schedule.tick skipped on market holiday jobType={}", JOB_COLLECT_INTRADAY);
            return;
        }
        String scheduledForText = EventFactory.format(scheduledFor);
        Envelope<TickPayload> tick = EventFactory.create(
                clock,
                "schedule.tick",
                scheduledFor,
                JOB_COLLECT_INTRADAY + "-" + scheduledForText,
                new TickPayload(JOB_COLLECT_INTRADAY, scheduledForText));
        kafka.send(Topics.SCHEDULE_TICK, JOB_COLLECT_INTRADAY, mapper.writeValueAsString(tick));
        log.info("published schedule.tick jobType={} scheduledFor={}", JOB_COLLECT_INTRADAY, scheduledForText);
    }

    // 타이머가 몇 초 늦게 불려도 끝 시각 tick 이 빠지지 않게 분 단위로 비교한다.
    private boolean withinHours(OffsetDateTime at) {
        ZonedDateTime korea = at.atZoneSameInstant(KOREA);
        DayOfWeek day = korea.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime time = korea.toLocalTime().truncatedTo(ChronoUnit.MINUTES);
        return !time.isBefore(properties.startTime()) && !time.isAfter(properties.endTime());
    }
}

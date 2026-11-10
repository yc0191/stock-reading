package dev.stockreading.collector.schedule;

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
 * 타이머는 한 곳뿐이다: 작업 시각이 되면 schedule.tick만 발행한다.
 * collector.schedule.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 * 시세 tick 은 평일 시세 시간대(collector.quote-hours) 안에서만, 뉴스 tick 은 늘 발행한다.
 * 휴장일이면(시세 제공자의 장 운영 상태로 판정) 시세 tick 을 발행하지 않는다. 판정하지 못하면 영업일로 본다.
 */
@Component
@ConditionalOnProperty(name = "collector.schedule.enabled", havingValue = "true")
class TickPublisher {

    static final String JOB_COLLECT = "collect";
    static final String JOB_COLLECT_NEWS = "collect-news";
    private static final ZoneId KOREA = ZoneId.of("Asia/Seoul");
    private static final Logger log = LoggerFactory.getLogger(TickPublisher.class);

    private final QuoteHoursProperties quoteHours;
    private final MarketDayPort marketDay;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    TickPublisher(QuoteHoursProperties quoteHours, MarketDayPort marketDay, KafkaTemplate<String, String> kafka,
                  ObjectMapper mapper, Clock clock) {
        this.quoteHours = quoteHours;
        this.marketDay = marketDay;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    @Scheduled(fixedRateString = "${collector.tick-interval-ms}")
    void publishCollectTick() {
        if (!withinQuoteHours(OffsetDateTime.now(clock))) {
            return;
        }
        // 시간대 안일 때만 묻는다. 결과는 하루 단위로 기억하므로 1초마다 불러도 조회는 하루 한 번이다.
        if (!marketDay.isBusinessDay()) {
            log.debug("schedule.tick skipped on market holiday jobType={}", JOB_COLLECT);
            return;
        }
        publishTick(JOB_COLLECT);
    }

    @Scheduled(fixedRateString = "${collector.schedule.news-interval-ms}")
    void publishCollectNewsTick() {
        publishTick(JOB_COLLECT_NEWS);
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

    // 끝 시각이 "15:30" 이면 15:30:59 까지 포함한다(분 단위 비교, 장중 수집과 같은 방식).
    private boolean withinQuoteHours(OffsetDateTime at) {
        ZonedDateTime korea = at.atZoneSameInstant(KOREA);
        DayOfWeek day = korea.getDayOfWeek();
        if (day == DayOfWeek.SATURDAY || day == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime time = korea.toLocalTime().truncatedTo(ChronoUnit.MINUTES);
        return !time.isBefore(quoteHours.startTime()) && !time.isAfter(quoteHours.endTime());
    }
}

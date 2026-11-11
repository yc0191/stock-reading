package dev.stockreading.collector.schedule;

import java.time.Clock;
import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.insight.IntradayProperties;
import dev.stockreading.collector.marketdata.MarketDayPort;
import dev.stockreading.collector.schedule.CollectorJobs.Job;

/**
 * 장중 수집의 타이머. cron 이 주기를 정하고, 평일과 시작·끝 시각(한국 시간) 밖이면 실행하지 않는다.
 * 휴장일(시세 제공자의 장 운영 상태로 판정, 판정하지 못하면 영업일)에는 실행하지 않는다. collector.intraday.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.intraday.enabled", havingValue = "true")
class IntradayTimer {

    private static final Logger log = LoggerFactory.getLogger(IntradayTimer.class);

    private final CollectorJobs jobs;
    private final MarketDayPort marketDay;
    private final IntradayProperties properties;
    private final Clock clock;

    IntradayTimer(CollectorJobs jobs, IntradayProperties properties, MarketDayPort marketDay, Clock clock) {
        this.jobs = jobs;
        this.marketDay = marketDay;
        this.properties = properties;
        this.clock = clock;
    }

    @Scheduled(cron = "${collector.intraday.cron}", zone = "Asia/Seoul")
    void collectIntraday() {
        OffsetDateTime now = OffsetDateTime.now(clock);
        if (!WeekdayHours.contains(now, properties.startTime(), properties.endTime())) {
            log.debug("intraday collect skipped outside hours at={}", now);
            return;
        }
        if (!marketDay.isBusinessDay()) {
            log.info("collect skipped on market holiday job={}", Job.COLLECT_INTRADAY);
            return;
        }
        jobs.run(Job.COLLECT_INTRADAY);
    }
}

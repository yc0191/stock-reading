package dev.stockreading.collector.schedule;

import java.time.Clock;
import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.marketdata.MarketDayPort;
import dev.stockreading.collector.schedule.CollectorJobs.Job;

/**
 * 1초 현재가와 뉴스 수집의 타이머. collector.schedule.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 * 현재가는 평일 시세 시간대(collector.quote-hours) 안에서만, 뉴스는 늘 수집한다.
 * 휴장일(시세 제공자의 장 운영 상태로 판정, 판정하지 못하면 영업일)에는 현재가를 조회하지 않는다. 뉴스는 휴장일에도 수집한다.
 */
@Component
@ConditionalOnProperty(name = "collector.schedule.enabled", havingValue = "true")
class CollectTimer {

    private static final Logger log = LoggerFactory.getLogger(CollectTimer.class);

    private final CollectorJobs jobs;
    private final QuoteHoursProperties quoteHours;
    private final MarketDayPort marketDay;
    private final Clock clock;

    CollectTimer(CollectorJobs jobs, QuoteHoursProperties quoteHours, MarketDayPort marketDay, Clock clock) {
        this.jobs = jobs;
        this.quoteHours = quoteHours;
        this.marketDay = marketDay;
        this.clock = clock;
    }

    // 조회는 작업 스레드에서 돌게 하고 타이머는 바로 돌아온다. 앞 회차 조회가 남아 있으면 이번 회차는 건너뜀으로 남는다.
    @Scheduled(fixedRateString = "${collector.tick-interval-ms}")
    void collect() {
        if (!WeekdayHours.contains(OffsetDateTime.now(clock), quoteHours.startTime(), quoteHours.endTime())) {
            return;
        }
        // 시간대 안일 때만 묻는다. 결과는 하루 단위로 기억하므로 1초마다 불러도 조회는 하루 한 번이다.
        if (!marketDay.isBusinessDay()) {
            log.debug("collect skipped on market holiday job={}", Job.COLLECT);
            return;
        }
        jobs.start(Job.COLLECT);
    }

    @Scheduled(fixedRateString = "${collector.schedule.news-interval-ms}")
    void collectNews() {
        jobs.run(Job.COLLECT_NEWS);
    }
}

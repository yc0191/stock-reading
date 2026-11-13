package dev.stockreading.collector.schedule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.marketdata.MarketDayPort;
import dev.stockreading.collector.schedule.CollectorJobs.Job;
import dev.stockreading.collector.trace.CorrelationId;

/**
 * 장 시작 전 수집의 타이머. 한 회차에서 해외 일봉·세계지수와 SEC 공시를 함께 받는다. 공시는 이때만 받는다.
 * 휴장일(시세 제공자의 장 운영 상태로 판정, 판정하지 못하면 영업일)에는 실행하지 않는다. collector.pre-open.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.pre-open.enabled", havingValue = "true")
class PreOpenTimer {

    private static final Logger log = LoggerFactory.getLogger(PreOpenTimer.class);

    private final CollectorJobs jobs;
    private final MarketDayPort marketDay;

    PreOpenTimer(CollectorJobs jobs, MarketDayPort marketDay) {
        this.jobs = jobs;
        this.marketDay = marketDay;
    }

    @Scheduled(cron = "${collector.pre-open.cron}", zone = "Asia/Seoul")
    void collectPreOpen() {
        // 메시지 처리 밖(타이머 스레드)에서 찍는 줄이라 이 실행의 correlationId 를 직접 로그 문맥에 둔다.
        CorrelationId.within(jobs.correlationId(Job.COLLECT_PRE_OPEN), () -> {
            if (!marketDay.isBusinessDay()) {
                log.info("collect skipped on market holiday job={}", Job.COLLECT_PRE_OPEN);
                return;
            }
            jobs.run(Job.COLLECT_PRE_OPEN);
        });
    }
}

package dev.stockreading.collector.schedule;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.schedule.CollectorJobs.Job;

/**
 * 장 시작 전 수집의 타이머. 한 회차에서 해외 일봉·세계지수와 SEC 공시를 함께 받는다. 공시는 이때만 받는다.
 * 휴장일은 판정하지 않는다. collector.pre-open.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.pre-open.enabled", havingValue = "true")
class PreOpenTimer {

    private final CollectorJobs jobs;

    PreOpenTimer(CollectorJobs jobs) {
        this.jobs = jobs;
    }

    @Scheduled(cron = "${collector.pre-open.cron}", zone = "Asia/Seoul")
    void collectPreOpen() {
        jobs.run(Job.COLLECT_PRE_OPEN);
    }
}

package dev.stockreading.collector.schedule;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.schedule.CollectorJobs.Job;

/**
 * 마감 후 수집의 타이머. 휴장일은 판정하지 않는다.
 * collector.insight.schedule.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.insight.schedule.enabled", havingValue = "true")
class PostCloseTimer {

    private final CollectorJobs jobs;

    PostCloseTimer(CollectorJobs jobs) {
        this.jobs = jobs;
    }

    @Scheduled(cron = "${collector.insight.schedule.cron}", zone = "Asia/Seoul")
    void collectPostClose() {
        jobs.run(Job.COLLECT_POST_CLOSE);
    }
}

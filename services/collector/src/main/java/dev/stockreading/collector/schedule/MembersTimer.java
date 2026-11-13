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
 * ETF 구성 종목 조회의 타이머. 평일 하루 한 번 돌고, 그날 실제로 받을 ETF(평소 주 1회, 지수 교체 달 매일)는 MembersPublisher 가 고른다.
 * 휴장일(시세 제공자의 장 운영 상태로 판정, 판정하지 못하면 영업일)에는 실행하지 않는다. collector.members.enabled 가 true 일 때만 빈이 만들어진다(기본 꺼짐).
 */
@Component
@ConditionalOnProperty(name = "collector.members.enabled", havingValue = "true")
class MembersTimer {

    private static final Logger log = LoggerFactory.getLogger(MembersTimer.class);

    private final CollectorJobs jobs;
    private final MarketDayPort marketDay;

    MembersTimer(CollectorJobs jobs, MarketDayPort marketDay) {
        this.jobs = jobs;
        this.marketDay = marketDay;
    }

    @Scheduled(cron = "${collector.members.cron}", zone = "Asia/Seoul")
    void collectMembers() {
        // 메시지 처리 밖(타이머 스레드)에서 찍는 줄이라 이 실행의 correlationId 를 직접 로그 문맥에 둔다.
        CorrelationId.within(jobs.correlationId(Job.COLLECT_ETF_MEMBERS), () -> {
            if (!marketDay.isBusinessDay()) {
                log.info("collect skipped on market holiday job={}", Job.COLLECT_ETF_MEMBERS);
                return;
            }
            jobs.run(Job.COLLECT_ETF_MEMBERS);
        });
    }
}

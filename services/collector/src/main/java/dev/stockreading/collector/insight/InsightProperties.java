package dev.stockreading.collector.insight;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 마감 후 조회 설정. 발행 시각(cron)은 schedule.enabled 가 true 일 때만 쓰인다.
 * continents 는 세계지수 조회에 쓰는 대륙구분 값 목록이다.
 */
@ConfigurationProperties("collector.insight")
public record InsightProperties(
        Schedule schedule, String dailyMarketClass, int dailyCount, List<String> continents) {

    public record Schedule(boolean enabled, String cron) {
    }
}

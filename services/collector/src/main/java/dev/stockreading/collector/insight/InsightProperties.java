package dev.stockreading.collector.insight;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 마감 후 조회 설정. 발행 시각(cron)은 schedule.enabled 가 true 일 때만 쓰인다.
 * continents 는 세계지수 조회에 쓰는 대륙구분 값 목록이다.
 * overseasMaxTickers 는 한 번의 마감 후 조회에서 해외 일봉을 조회할 종목 수 상한이고, 넘친 종목은 건너뛴다.
 */
@ConfigurationProperties("collector.insight")
public record InsightProperties(
        Schedule schedule, String dailyMarketClass, int dailyCount, List<String> continents,
        int overseasDailyCount, int overseasMaxTickers) {

    static final int DEFAULT_OVERSEAS_DAILY_COUNT = 30;
    static final int DEFAULT_OVERSEAS_MAX_TICKERS = 100;

    public InsightProperties {
        if (overseasDailyCount <= 0) {
            overseasDailyCount = DEFAULT_OVERSEAS_DAILY_COUNT;
        }
        if (overseasMaxTickers <= 0) {
            overseasMaxTickers = DEFAULT_OVERSEAS_MAX_TICKERS;
        }
    }

    public record Schedule(boolean enabled, String cron) {
    }
}

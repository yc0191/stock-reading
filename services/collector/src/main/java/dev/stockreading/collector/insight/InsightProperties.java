package dev.stockreading.collector.insight;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 마감 후 조회 설정. 발행 시각(cron)은 schedule.enabled 가 true 일 때만 쓰인다.
 * continents 는 세계지수 조회에 쓰는 대륙구분 값 목록이다.
 * overseasMaxTickers 는 한 번의 마감 후 조회에서 해외 일봉을 조회할 종목 수 상한이고, 넘친 종목은 건너뛴다.
 * retry 는 마감 후 조회에서 실패한 조회만 다시 부르는 횟수와 간격이다.
 */
@ConfigurationProperties("collector.insight")
public record InsightProperties(
        Schedule schedule, String dailyMarketClass, int dailyCount, List<String> continents,
        int overseasDailyCount, int overseasMaxTickers, Retry retry) {

    static final int DEFAULT_OVERSEAS_DAILY_COUNT = 30;
    static final int DEFAULT_OVERSEAS_MAX_TICKERS = 100;
    static final Retry DEFAULT_RETRY = new Retry(3, Duration.ofMinutes(6));

    public InsightProperties {
        if (overseasDailyCount <= 0) {
            overseasDailyCount = DEFAULT_OVERSEAS_DAILY_COUNT;
        }
        if (overseasMaxTickers <= 0) {
            overseasMaxTickers = DEFAULT_OVERSEAS_MAX_TICKERS;
        }
        if (retry == null) {
            retry = DEFAULT_RETRY;
        }
    }

    public record Schedule(boolean enabled, String cron) {
    }

    /**
     * 회차 첫 조회에서 실패한 조회만 회차 시작 시각에서 interval 의 배수 시각에 최대 maxAttempts 번 다시 부른다.
     * 0 번이면 다시 부르지 않는다.
     */
    public record Retry(int maxAttempts, Duration interval) {

        public Retry {
            if (maxAttempts < 0) {
                throw new IllegalArgumentException("collector.insight.retry.max-attempts 는 0 이상이어야 한다");
            }
            if (interval == null || interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("collector.insight.retry.interval 은 0 보다 길어야 한다");
            }
        }
    }
}

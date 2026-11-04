package dev.stockreading.market.context;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 데일리 판 자료 응답(market.daily)의 안전 상한. 기간·원문 크기·티커 수·세계지수 대륙은 배경 자료 응답(market.context)
 * 설정을 함께 쓰고, 여기에는 데일리 응답에만 있는 상한을 둔다.
 *
 * @param maxEtfs 응답에 담을 보유 ETF의 최대 개수
 */
@ConfigurationProperties("market.daily")
public record DailyProperties(int maxEtfs) {

    public DailyProperties {
        // 상한이 비면 모든 ETF가 잘려 조용히 빈 응답이 나가므로 시작 단계에서 막는다.
        if (maxEtfs < 1) {
            throw new IllegalArgumentException("market.daily.max-etfs must be positive");
        }
    }
}

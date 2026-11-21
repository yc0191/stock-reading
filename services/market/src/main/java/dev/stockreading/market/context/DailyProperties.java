package dev.stockreading.market.context;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 데일리 판 자료 응답(market.daily)의 안전 상한. 기간·원문 크기·티커 수·세계지수 대륙은 배경 자료 응답(market.context)
 * 설정을 함께 쓰고, 여기에는 데일리 응답에만 있는 상한을 둔다.
 *
 * @param maxEtfs 응답에 담을 보유 ETF의 최대 개수
 * @param openFlowMaxWindow 개장 흐름이 판 기준 시각부터 거슬러 읽는 시세 구간의 최대 길이. 비우면 30분
 * @param intradayMaxWindow 장중 흐름(1분 간격 시세)이 판 기준 시각부터 거슬러 읽는 시세 구간의 최대 길이. 비우면 9시간
 * @param intradayMaxPoints 장중 흐름에 ETF마다 담는 최대 점 수(1분에 한 점)
 * @param dailyClosesMax 기간 종가에 ETF마다 담는 최대 거래일 수
 * @param dailyClosesLookback 기간 종가를 모을 때 판 기준 시각부터 거슬러 읽는 국내 일봉 원문의 받은 시각 범위. 비우면 200일
 * @param dailyClosesMaxResponses 기간 종가를 모을 때 읽는 국내 일봉 원문의 최대 건수(받은 날마다 한 건, 최근 것부터). 비우면 7
 */
@ConfigurationProperties("market.daily")
public record DailyProperties(int maxEtfs, Duration openFlowMaxWindow, Duration intradayMaxWindow,
        int intradayMaxPoints, int dailyClosesMax, Duration dailyClosesLookback, Integer dailyClosesMaxResponses) {

    public DailyProperties {
        // 상한이 비면 모든 ETF가 잘려 조용히 빈 응답이 나가므로 시작 단계에서 막는다.
        if (maxEtfs < 1) {
            throw new IllegalArgumentException("market.daily.max-etfs must be positive");
        }
        // 점 수 상한이 비면 요청한 흐름·종가가 늘 빈 칸으로 나가므로 시작 단계에서 막는다.
        if (intradayMaxPoints < 1) {
            throw new IllegalArgumentException("market.daily.intraday-max-points must be positive");
        }
        if (dailyClosesMax < 1) {
            throw new IllegalArgumentException("market.daily.daily-closes-max must be positive");
        }
        if (openFlowMaxWindow == null) {
            openFlowMaxWindow = Duration.ofMinutes(30);
        }
        if (intradayMaxWindow == null) {
            intradayMaxWindow = Duration.ofHours(9);
        }
        if (dailyClosesLookback == null) {
            dailyClosesLookback = Duration.ofDays(200);
        }
        if (dailyClosesMaxResponses == null) {
            dailyClosesMaxResponses = 7;
        }
        // 0이면 기간 종가가 늘 비어 그래프가 조용히 빠지므로 시작 단계에서 막는다.
        if (dailyClosesMaxResponses < 1) {
            throw new IllegalArgumentException("market.daily.daily-closes-max-responses must be positive");
        }
    }
}

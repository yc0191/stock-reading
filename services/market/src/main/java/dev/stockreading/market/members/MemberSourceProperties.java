package dev.stockreading.market.members;

import java.time.DayOfWeek;
import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * ETF 구성 종목 자동 조회 설정. 출처 주소와 ETF별 번호는 저장소에 두지 않고 환경변수(주소)와 etf 표(출처 번호)로만 받는다.
 *
 * <p>urlTemplate이 비어 있으면 자동 조회를 하지 않는다(구성 종목은 etf_stock_map을 직접 고친다).
 * urlTemplate에는 {@code {fundId}}와 {@code {date}}(yyyy.MM.dd) 자리가 있다. ETF별 출처 번호는 환경변수가 아니라 etf.constituent_source_id에 있다.
 */
@ConfigurationProperties("market.members")
public record MemberSourceProperties(String urlTemplate, String userAgent, int topCount,
        Duration callInterval, Duration timeout, DayOfWeek fetchDay) {

    public MemberSourceProperties {
        topCount = topCount < 1 ? 10 : topCount;
        callInterval = callInterval == null ? Duration.ZERO : callInterval;
        timeout = timeout == null ? Duration.ofSeconds(10) : timeout;
        fetchDay = fetchDay == null ? DayOfWeek.MONDAY : fetchDay;
        userAgent = userAgent == null || userAgent.isBlank() ? "stock-reading-members/1.0" : userAgent;
    }

    boolean enabled() {
        return urlTemplate != null && !urlTemplate.isBlank();
    }

    // 주소와 보유 ETF가 드러나므로 값을 로그에 남기지 않는다.
    @Override
    public String toString() {
        return "MemberSourceProperties[***]";
    }
}

package dev.stockreading.market.members;

import java.time.DayOfWeek;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * ETF 구성 종목 자동 조회 설정. 출처 주소와 ETF별 번호는 저장소에 두지 않고 환경변수로만 받는다.
 *
 * <p>urlTemplate이 비어 있으면 자동 조회를 하지 않는다(구성 종목은 종목 테이블을 직접 고친다).
 * urlTemplate에는 {@code {fundId}}와 {@code {date}}(yyyy.MM.dd) 자리가 있다. fundIds는 "ETF코드:번호"를 쉼표로 이은 값이다.
 */
@ConfigurationProperties("market.members")
public record MemberSourceProperties(String urlTemplate, String fundIds, String userAgent, int topCount,
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

    /** ETF코드 → 출처 번호. 모양이 틀린 항목은 건너뛴다. */
    Map<String, String> fundIdByEtf() {
        Map<String, String> result = new LinkedHashMap<>();
        if (fundIds == null) {
            return result;
        }
        for (String pair : fundIds.split(",")) {
            String[] parts = pair.split(":", 2);
            if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                result.put(parts[0].strip(), parts[1].strip());
            }
        }
        return result;
    }

    // 주소와 보유 ETF가 드러나므로 값을 로그에 남기지 않는다.
    @Override
    public String toString() {
        return "MemberSourceProperties[***]";
    }
}

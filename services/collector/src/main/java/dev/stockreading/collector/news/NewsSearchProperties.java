package dev.stockreading.collector.news;

import org.springframework.boot.context.properties.ConfigurationProperties;

// idHeader, secretHeader 는 제공자가 정한 인증 헤더 이름이고, 값은 환경변수로 받는다.
// dailyCallLimit 은 하루(한국 시간 기준) 검색 API 호출 상한이며, 0 이하는 설정 오류로 시작 단계에서 막는다.
// callIntervalMs 는 검색어를 연달아 호출할 때 호출 사이에 두는 시간이다. 0이면 두지 않는다.
@ConfigurationProperties("collector.news-search")
public record NewsSearchProperties(
        String baseUrl, String clientId, String clientSecret, String idHeader, String secretHeader,
        int display, int timeoutSeconds, int dailyCallLimit, long callIntervalMs) {

    public NewsSearchProperties {
        if (dailyCallLimit < 1) {
            throw new IllegalArgumentException("collector.news-search.daily-call-limit 는 1 이상이어야 합니다");
        }
        if (callIntervalMs < 0) {
            throw new IllegalArgumentException("collector.news-search.call-interval-ms 는 0 이상이어야 합니다");
        }
    }

    // 클라이언트 아이디와 시크릿이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "NewsSearchProperties[baseUrl=" + baseUrl + ", clientId=***, clientSecret=***, display=" + display + "]";
    }
}

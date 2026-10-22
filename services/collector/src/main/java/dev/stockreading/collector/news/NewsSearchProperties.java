package dev.stockreading.collector.news;

import org.springframework.boot.context.properties.ConfigurationProperties;

// idHeader, secretHeader 는 제공자가 정한 인증 헤더 이름이고, 값은 환경변수로 받는다.
// queries 는 "검색어=종목코드,종목코드;검색어2=*" 형식이며 관심 종목이 드러나므로 저장소에 두지 않는다.
@ConfigurationProperties("collector.news-search")
public record NewsSearchProperties(
        String baseUrl, String clientId, String clientSecret, String idHeader, String secretHeader,
        int display, int timeoutSeconds, String queries) {

    // 클라이언트 아이디와 시크릿이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "NewsSearchProperties[baseUrl=" + baseUrl + ", clientId=***, clientSecret=***, display=" + display + "]";
    }
}

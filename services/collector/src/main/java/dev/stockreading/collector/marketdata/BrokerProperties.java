package dev.stockreading.collector.marketdata;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector.broker")
public record BrokerProperties(String baseUrl, String appKey, String appSecret, int timeoutSeconds,
                               int maxCallsPerSecond, int quoteTimeoutSeconds) {

    // 실제 서버에서 초당 12회는 5분을 버텼고 13회부터는 1분 안에 한도 초과가 났다(실측). 여유를 두어 10으로 둔다.
    static final int DEFAULT_MAX_CALLS_PER_SECOND = 10;

    // 현재가는 1초마다 부르므로 오래 기다리지 않고 끊는다. 토큰 발급과 그 밖의 조회는 timeoutSeconds 를 응답 대기에만 쓴다(연결 맺기는 모든 요청에 이 값).
    static final int DEFAULT_QUOTE_TIMEOUT_SECONDS = 2;

    public BrokerProperties {
        if (maxCallsPerSecond <= 0) {
            maxCallsPerSecond = DEFAULT_MAX_CALLS_PER_SECOND;
        }
        if (quoteTimeoutSeconds <= 0) {
            quoteTimeoutSeconds = DEFAULT_QUOTE_TIMEOUT_SECONDS;
        }
    }

    // 키와 시크릿이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "BrokerProperties[baseUrl=" + baseUrl + ", appKey=***, appSecret=***]";
    }
}

package dev.stockreading.collector.marketdata;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector.broker")
public record BrokerProperties(String baseUrl, String appKey, String appSecret, int timeoutSeconds,
                               int maxCallsPerSecond) {

    static final int DEFAULT_MAX_CALLS_PER_SECOND = 10;

    public BrokerProperties {
        if (maxCallsPerSecond <= 0) {
            maxCallsPerSecond = DEFAULT_MAX_CALLS_PER_SECOND;
        }
    }

    // 키와 시크릿이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "BrokerProperties[baseUrl=" + baseUrl + ", appKey=***, appSecret=***]";
    }
}

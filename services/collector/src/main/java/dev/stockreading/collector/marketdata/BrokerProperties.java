package dev.stockreading.collector.marketdata;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector.broker")
public record BrokerProperties(String baseUrl, String appKey, String appSecret, int timeoutSeconds) {

    // 키와 시크릿이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "BrokerProperties[baseUrl=" + baseUrl + ", appKey=***, appSecret=***]";
    }
}

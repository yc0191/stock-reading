package dev.stockreading.collector.disclosure;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("collector.dart")
public record DartProperties(String baseUrl, String apiKey, int pageCount, int timeoutSeconds) {

    // 인증키가 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "DartProperties[baseUrl=" + baseUrl + ", apiKey=***, pageCount=" + pageCount + "]";
    }
}

package dev.stockreading.collector.marketdata;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 장 운영 상태 조회 설정. 제공자 고유의 경로와 응답 칸 이름은 공개 저장소에 두지 않고 환경변수로 받는다.
 * 값이 비어 있으면 휴장일을 판정하지 않고 늘 영업일로 본다.
 */
@ConfigurationProperties("collector.market-status")
public record MarketStatusProperties(String path, String todayField, String businessDayField, Duration lookupTimeout) {

    static final Duration DEFAULT_LOOKUP_TIMEOUT = Duration.ofSeconds(5);

    public MarketStatusProperties {
        path = path == null ? "" : path.trim();
        todayField = todayField == null ? "" : todayField.trim();
        businessDayField = businessDayField == null ? "" : businessDayField.trim();
        if (lookupTimeout == null || lookupTimeout.isZero() || lookupTimeout.isNegative()) {
            lookupTimeout = DEFAULT_LOOKUP_TIMEOUT;
        }
    }

    boolean configured() {
        return !path.isEmpty() && !todayField.isEmpty() && !businessDayField.isEmpty();
    }
}

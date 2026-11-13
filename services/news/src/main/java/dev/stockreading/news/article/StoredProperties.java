package dev.stockreading.news.article;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param waitLimit 수집 완료를 받은 뒤 그 기사들이 처리되기를 기다리는 한도. 넘으면 처리되지 않은 기사를 담아 저장 끝을 낸다.
 *                  브리핑의 재검색 기다림 한도(5분)보다 짧아야 브리핑이 이 알림을 받는다.
 * @param pollInterval 기다리는 동안 처리 결과를 다시 읽는 간격.
 * @param staleAfter 수집 끝 시각(finishedAt)에서 이만큼 지난 완료는 기다리지 않고 지금 결과로 낸다. 없으면 waitLimit 과 같다.
 */
@ConfigurationProperties("news.stored")
public record StoredProperties(
        @DefaultValue("60s") Duration waitLimit,
        @DefaultValue("1s") Duration pollInterval,
        Duration staleAfter) {

    public StoredProperties {
        if (staleAfter == null) {
            staleAfter = waitLimit;
        }
    }
}

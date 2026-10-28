package dev.stockreading.market;

import java.math.BigDecimal;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param windowMinutes 유형 1(window)이 현재 시세와 비교할 최근 이력의 길이(분).
 * @param windowThresholdPercent 유형 1에서 창의 가장 오래된 스냅샷 대비 이 값(%) 이상 변하면(절댓값) "변동"으로 본다.
 * @param prevCloseField 유형 2(prevClose)가 응답 원문(raw)에서 읽을 전일 종가 필드. 점(.)으로 중첩 경로를 쓴다.
 * @param prevCloseThresholdPercent 유형 2에서 전일 종가 대비 이 값(%) 이상 변하면(절댓값) "변동"으로 본다.
 */
@ConfigurationProperties("market")
public record MarketProperties(
        int windowMinutes,
        BigDecimal windowThresholdPercent,
        String prevCloseField,
        BigDecimal prevCloseThresholdPercent) {
}

package dev.stockreading.market;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param windowMinutes 유형 1(window)이 현재 시세와 비교할 최근 이력의 길이(분).
 * @param windowThresholdPercent 유형 1에서 창의 가장 오래된 스냅샷 대비 이 값(%) 이상 변하면(절댓값) "변동"으로 본다.
 * @param prevCloseFields 유형 2(prevClose)가 응답 원문(raw)에서 읽을 전일 종가 후보 필드들. 점(.)으로 중첩 경로를 쓰고,
 *        절반을 넘는 필드가 같은 값일 때만 그 값을 전일 종가로 쓴다. 비우면 유형 2를 끈다.
 * @param prevCloseThresholdPercent 유형 2에서 전일 종가 대비 이 값(%) 이상 변하면(절댓값) "변동"으로 본다.
 * @param openPriceFields 개장 흐름이 응답 원문(raw)에서 읽을 시가 칸. 점(.)으로 중첩 경로를 쓰고, 앞에서부터 처음 읽히는 값을 쓴다.
 *        읽히는 값이 없으면 개장 흐름은 09:00 이후 첫 스냅샷 가격을 시가로 쓰고 그 사실을 표시한다.
 */
@ConfigurationProperties("market")
public record MarketProperties(
        int windowMinutes,
        BigDecimal windowThresholdPercent,
        List<String> prevCloseFields,
        BigDecimal prevCloseThresholdPercent,
        List<String> openPriceFields) {
}

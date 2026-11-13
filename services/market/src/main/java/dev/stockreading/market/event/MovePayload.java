package dev.stockreading.market.event;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * detectionType, moveStartedAt, todayChangePercent는 선택 필드다. null이면 직렬화에서 빠져 이전 형식과 같아진다.
 * todayChangePercent는 감지한 스냅샷 원문의 전일 종가 대비 변동률이다(응답의 등락률 칸이 아니라 코드가 계산).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MovePayload(
        String symbol,
        long previousPrice,
        long price,
        BigDecimal changePercent,
        BigDecimal thresholdPercent,
        String asOf,
        String detectionType,
        String moveStartedAt,
        BigDecimal todayChangePercent) {

    public MovePayload(
            String symbol, long previousPrice, long price, BigDecimal changePercent,
            BigDecimal thresholdPercent, String asOf) {
        this(symbol, previousPrice, price, changePercent, thresholdPercent, asOf, null, null, null);
    }
}

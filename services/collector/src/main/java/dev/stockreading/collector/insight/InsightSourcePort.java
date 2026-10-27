package dev.stockreading.collector.insight;

/**
 * 마감 후 조회 출처. 응답 본문 원문(앞뒤 공백만 제거)을 돌려주고, 실패하면 예외를 던진다.
 * 조회만 하며 주문·계좌 호출은 두지 않는다(ADR-007).
 */
public interface InsightSourcePort {

    String dailyPrice(String symbol);

    String worldIndex(String continent);

    String exchangeRateSummary();

    String marketSummary();
}

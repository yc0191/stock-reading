package dev.stockreading.collector.marketdata;

/**
 * 오늘이 영업일인지 알려 준다. 조회만 하며 주문·계좌 호출은 두지 않는다(ADR-007).
 * 판정할 수 없을 때는 영업일로 본다. 휴장일에 tick 을 잘못 보내는 편이 영업일 tick 을 놓치는 것보다 낫기 때문이다.
 */
public interface MarketDayPort {

    boolean isBusinessDay();
}

package dev.stockreading.market.members;

/**
 * ETF 상위 구성 종목 한 개. rank는 비중 순위(1이 가장 큼)이고, weight는 출처가 준 %를 앞뒤 공백만 걷은 문자열이다.
 * name과 weight는 모르면 null이다.
 */
public record Member(String market, String symbol, String name, String weight, int rank) {
}

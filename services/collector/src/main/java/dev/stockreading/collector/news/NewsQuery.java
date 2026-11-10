package dev.stockreading.collector.news;

import java.util.Set;

/** 검색어 하나와 그 검색어로 찾은 기사에 태그로 붙일 종목코드들. 종목 자리의 "*"는 시장 전체 이슈다. */
public record NewsQuery(String query, Set<String> symbols) {

    // 시장 전체 이슈는 종목 자리에 이 값을 쓴다.
    public static final String MARKET_WIDE = "*";
}

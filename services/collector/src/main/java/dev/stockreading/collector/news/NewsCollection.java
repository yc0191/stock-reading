package dev.stockreading.collector.news;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

/**
 * 한 번의 수집 결과. incompleteSymbols 는 제대로 모으지 못했을 수 있는 종목코드이며(검색어 호출 실패,
 * 일일 호출 상한으로 건너뜀), 시장 전체 검색어가 그렇게 되면 "*" 가 들어 있다.
 * startedAt 은 첫 검색 호출을 시작한 시각이다.
 */
public record NewsCollection(List<NewsItem> items, Set<String> incompleteSymbols, OffsetDateTime startedAt) {
}

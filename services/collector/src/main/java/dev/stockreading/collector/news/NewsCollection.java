package dev.stockreading.collector.news;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;

/**
 * 한 번의 수집 결과. incompleteSymbols 는 제대로 모으지 못했을 수 있는 종목코드이며(검색어 호출 실패,
 * 일일 호출 상한으로 건너뜀), 시장 전체 검색어가 그렇게 되면 "*" 가 들어 있다.
 * startedAt 은 첫 검색 호출을 시작한 시각이다.
 * searchedQueryCount·reusedQueryCount 는 재검색에서 직접 검색한(실패 포함) 검색어 수와 최근 결과를 다시 쓴 검색어 수이며,
 * 정기 수집이거나 세지 못했으면 null 이다. 하루 호출 상한으로 건너뛴 검색어는 어느 쪽에도 들지 않는다.
 */
public record NewsCollection(List<NewsItem> items, Set<String> incompleteSymbols, OffsetDateTime startedAt,
        Integer searchedQueryCount, Integer reusedQueryCount) {

    public NewsCollection(List<NewsItem> items, Set<String> incompleteSymbols, OffsetDateTime startedAt) {
        this(items, incompleteSymbols, startedAt, null, null);
    }
}

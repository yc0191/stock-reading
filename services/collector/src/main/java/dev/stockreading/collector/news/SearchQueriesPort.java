package dev.stockreading.collector.news;

import java.util.List;

/**
 * 정기 뉴스 수집이 쓸 검색어 목록. 목록은 DB의 검색 대상 뷰에서 시작할 때 한 번 읽는다.
 * 설정 값으로 대신하지 않으며, 바뀐 값은 재시작해야 반영된다.
 */
public interface SearchQueriesPort {

    /** 시작할 때 읽은 검색어 전체. */
    List<NewsQuery> queries();
}

package dev.stockreading.collector.news;

import java.util.List;

/**
 * 정기 뉴스 수집이 쓸 검색어 목록. 목록은 뉴스 서비스가 알리는 news.search-queries 이벤트로만 받는다.
 * 설정 값으로 대신하지 않으므로 목록을 받기 전에는 예외를 던진다(빈 목록과 구별하기 위해서다).
 */
public interface SearchQueriesPort {

    /** 가장 최근에 받은 검색어 전체. 아직 받지 못했으면 IllegalStateException 이다. */
    List<NewsQuery> queries();
}

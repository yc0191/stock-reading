package dev.stockreading.collector.news;

import java.util.List;

public interface NewsSearchPort {

    /** 검색어로 최신순 기사를 가져온다. 반환 항목의 symbols 는 비어 있고 채우는 일은 NewsCollector 가 한다. */
    List<NewsItem> search(String query, int display);
}

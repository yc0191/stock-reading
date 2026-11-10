package dev.stockreading.news.searchquery;

import org.springframework.boot.context.properties.ConfigurationProperties;

// file 은 "검색어=종목코드,종목코드;검색어2=*" 글이 담긴 UTF-8 파일의 경로이고, 관심 종목이 드러나므로 파일은 저장소 밖에 둔다.
// 검색어 테이블이 비어 있을 때 처음 채우는 데만 쓴다. 테이블이 바뀌었는지 확인하는 간격(news.search-queries.check-interval)은 SearchQueriesChangeWatcher 가 설정에서 바로 읽는다.
@ConfigurationProperties("news.search-queries")
public record SearchQueryProperties(String file) {

    // 설정 값이 로그에 남지 않게 toString 에서 가린다.
    @Override
    public String toString() {
        return "SearchQueryProperties[file=***]";
    }
}

package dev.stockreading.news.searchquery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import dev.stockreading.news.event.SearchQueriesPayload;

/**
 * 시작할 때 검색어 테이블이 비어 있으면 검색어 파일(news.search-queries.file)로 처음 한 번 채운다.
 * 테이블에 행이 있으면 파일을 읽지도 않는다. 그 뒤의 고침은 테이블을 직접 고치며, 파일이 이미 있는 행을
 * 덮어쓰거나 지우면 직접 고친 내용이 재시작 때 사라지기 때문이다. 파일이 없거나 비어 있어도 시작에는 영향이 없다.
 * 카프카 리스너가 시작하기 전에 끝나도록 모든 빈이 만들어진 직후에 돈다.
 */
@Component
class SearchQuerySeeder implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(SearchQuerySeeder.class);

    private final SearchQueryProperties properties;
    private final SearchQueryRepository repository;

    SearchQuerySeeder(SearchQueryProperties properties, SearchQueryRepository repository) {
        this.properties = properties;
        this.repository = repository;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (!repository.isEmpty()) {
            log.info("검색어 테이블에 이미 행이 있어 파일로 채우지 않습니다");
            return;
        }
        List<SearchQueriesPayload.Query> queries = SearchQuerySpec.parse(read(properties.file()));
        repository.insertAll(queries);
        log.info("검색어 테이블이 비어 있어 파일로 처음 채웠습니다: 검색어 {}개", queries.size());
    }

    // 파일이 없거나 못 읽는 것도 실패다. 경로는 메시지에 넣지 않는다.
    private static String read(String file) {
        if (file == null || file.isBlank()) {
            return "";
        }
        try {
            return Files.readString(Path.of(file.strip()), StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("검색어 파일을 읽을 수 없습니다");
        }
    }
}

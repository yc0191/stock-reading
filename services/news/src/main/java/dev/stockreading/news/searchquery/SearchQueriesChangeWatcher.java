package dev.stockreading.news.searchquery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 검색어 테이블을 주기적으로 읽어 내용이 바뀌었을 때만 전체 목록 이벤트를 발행한다(재시작 없이 반영).
 * 시작한 뒤 첫 확인은 마지막으로 보낸 해시를 모르므로 한 번 무조건 발행한다. 토픽이 압축되어 있고
 * 소비자가 같은 목록에 멱등하므로 중복은 해가 없고, 새 토픽이나 비워진 토픽도 채워진다.
 */
@Component
class SearchQueriesChangeWatcher {

    private static final Logger log = LoggerFactory.getLogger(SearchQueriesChangeWatcher.class);

    private final SearchQueryRepository repository;
    private final SearchQueriesPublisher publisher;

    // 발행에 성공한 목록의 해시. 발행이 실패하면 갱신하지 않아 다음 확인 때 다시 보낸다.
    private String lastPublishedHash;

    SearchQueriesChangeWatcher(SearchQueryRepository repository, SearchQueriesPublisher publisher) {
        this.repository = repository;
        this.publisher = publisher;
    }

    @Scheduled(initialDelayString = "${news.search-queries.check-interval}",
            fixedDelayString = "${news.search-queries.check-interval}")
    void check() {
        try {
            SearchQuerySnapshot snapshot = repository.readSnapshot();
            String hash = snapshot.contentHash();
            if (hash.equals(lastPublishedHash)) {
                return;
            }
            publisher.publish(snapshot);
            lastPublishedHash = hash;
        } catch (Exception e) {
            // 한 번 실패해도 다음 확인에서 다시 시도한다. 원인 메시지에 값이 섞일 수 있어 종류만 남긴다.
            log.warn("search queries check failed errorType={}", e.getClass().getSimpleName());
        }
    }
}

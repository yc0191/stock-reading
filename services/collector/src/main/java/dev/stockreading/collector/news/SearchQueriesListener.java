package dev.stockreading.collector.news;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.SearchQueriesEnvelope;
import tools.jackson.databind.ObjectMapper;

/**
 * 뉴스 서비스가 알리는 검색어 목록(news.search-queries)을 받아 최근 목록으로 바꾼다.
 * 이 토픽은 마지막 목록만 남는 압축 토픽이라, 파티션을 배정받을 때마다 처음부터 읽어야 마지막 목록을 받는다.
 */
@Component
class SearchQueriesListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(SearchQueriesListener.class);

    private final ReceivedSearchQueries queries;
    private final ObjectMapper mapper;

    SearchQueriesListener(ReceivedSearchQueries queries, ObjectMapper mapper) {
        this.queries = queries;
        this.mapper = mapper;
    }

    // 읽은 위치가 이미 커밋된 그룹은 auto.offset.reset 이 쓰이지 않아 재시작하면 마지막 목록을 다시 받지 못한다.
    // 목록은 메모리에만 두므로 파티션을 배정받을 때마다 처음부터 읽는다(키 하나인 압축 토픽이라 읽을 양은 거의 없다).
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        callback.seekToBeginning(assignments.keySet());
    }

    // 다른 리스너들과 그룹을 나눠 둔다. 같은 그룹이면 토픽 배정이 섞여 어느 한쪽이 메시지를 놓칠 수 있다.
    @KafkaListener(topics = Topics.NEWS_SEARCH_QUERY_LIST, groupId = "collector-search-queries",
            properties = "auto.offset.reset=earliest")
    void onQueries(String message) {
        SearchQueriesEnvelope event = mapper.readValue(message, SearchQueriesEnvelope.class);
        if (event.payload() == null || event.payload().queries() == null) {
            // 깨진 목록으로 지금 목록을 지우지 않고 이전 목록을 그대로 둔다.
            log.warn("invalid search queries event ignored eventId={} corr={}", event.eventId(), event.correlationId());
            return;
        }
        queries.update(event.payload());
    }
}

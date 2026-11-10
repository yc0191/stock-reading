package dev.stockreading.news.searchquery;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.Envelope;
import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.event.SearchQueriesPayload;
import tools.jackson.databind.ObjectMapper;

@Component
class SearchQueriesPublisher {

    private static final Logger log = LoggerFactory.getLogger(SearchQueriesPublisher.class);

    /** 압축 토픽이라 키가 같아야 마지막 목록 하나만 남는다. */
    static final String KEY = "search-queries";

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    SearchQueriesPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * 전체 목록을 발행하고 브로커가 받을 때까지 기다린다. 실패하면 예외를 던져 호출한 쪽이 다음 확인 때 다시 보내게 한다.
     * 검색어와 종목은 관심 종목이 드러나므로 건수만 남긴다.
     */
    void publish(SearchQuerySnapshot snapshot) throws Exception {
        // 요청이 없는 이벤트라 이어받을 correlationId가 없어 확인 회차마다 새로 만든다.
        Envelope<SearchQueriesPayload> event = EventFactory.create(clock, Topics.NEWS_SEARCH_QUERY_LIST,
                OffsetDateTime.now(clock), "search-queries-change-" + UUID.randomUUID(), snapshot.toPayload());
        kafka.send(Topics.NEWS_SEARCH_QUERY_LIST, KEY, mapper.writeValueAsString(event))
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info("search queries published queries={} corr={}", snapshot.queries().size(), event.correlationId());
    }
}

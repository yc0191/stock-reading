package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.List;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.event.SearchQueriesEnvelope.Query;
import dev.stockreading.collector.event.SearchQueriesEnvelope.SearchQueriesPayload;
import tools.jackson.databind.json.JsonMapper;

/** 가짜 값(TEST 접두 종목코드, 가짜 검색어)만 쓴다. */
class ReceivedSearchQueriesTest {

    private final ReceivedSearchQueries received = new ReceivedSearchQueries();

    @Test
    void beforeAnyListIsReceivedAskingForQueriesFailsInsteadOfReturningAnEmptyList() {
        assertThatThrownBy(received::queries).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void receivedQueriesKeepTheirTagsIncludingMarketWideAndEachListReplacesThePreviousOne() {
        received.update(new SearchQueriesPayload("h1", List.of(
                new Query("테스트 검색어", List.of("TEST01", "TEST02")), new Query("테스트 시황", List.of("*")))));

        assertThat(received.queries()).extracting(NewsQuery::query).containsExactly("테스트 검색어", "테스트 시황");
        assertThat(received.queries().get(0).symbols()).containsExactly("TEST01", "TEST02");
        assertThat(received.queries().get(1).symbols()).containsExactly("*");

        received.update(new SearchQueriesPayload("h2", List.of()));
        assertThat(received.queries()).isEmpty();
    }

    @Test
    void tagsTheArticleEventCannotCarryAreDroppedAndAQueryWithNoTagLeftIsDropped() {
        // 미국 티커 태그는 기사 이벤트의 종목코드 형식(6자리 코드 또는 *)에 맞지 않아 기사가 통째로 버려지므로 받을 때 거른다.
        received.update(new SearchQueriesPayload("h", List.of(
                new Query("섞인 태그", List.of("TEST01", "FAKE-B", "fake")),
                new Query("미국만", List.of("FAKE")),
                new Query("  ", List.of("TEST01")),
                new Query("태그 없음", List.of()))));

        assertThat(received.queries()).hasSize(1);
        assertThat(received.queries().get(0).query()).isEqualTo("섞인 태그");
        assertThat(received.queries().get(0).symbols()).containsExactly("TEST01");
    }

    @Test
    void theCommittedSampleThroughTheListenerBecomesTheReceivedList() throws IOException {
        SearchQueriesListener listener = new SearchQueriesListener(received, JsonMapper.builder().build());

        listener.onQueries(ContractSchemas.sample("news.search-queries.sample.json"));

        assertThat(received.queries()).extracting(NewsQuery::query)
                .containsExactly("테스트 ETF 하나", "Fake Corp", "가짜 시장 이슈");
    }

    @Test
    void aBrokenEventKeepsThePreviousList() throws IOException {
        SearchQueriesListener listener = new SearchQueriesListener(received, JsonMapper.builder().build());
        listener.onQueries(ContractSchemas.sample("news.search-queries.sample.json"));

        listener.onQueries("{\"eventId\":\"x\",\"schemaVersion\":1,\"payload\":{\"contentHash\":\"h\"}}");

        assertThat(received.queries()).hasSize(3);
    }

    @Test
    void theListenerUsesItsOwnGroupAndReadsFromTheBeginning() throws Exception {
        Method method = SearchQueriesListener.class.getDeclaredMethod("onQueries", String.class);
        KafkaListener annotation = method.getAnnotation(KafkaListener.class);

        assertThat(annotation.topics()).containsExactly("news.search-queries");
        assertThat(annotation.groupId()).isEqualTo("collector-search-queries");
        assertThat(annotation.properties()).contains("auto.offset.reset=earliest");
    }

    @Test
    void aGroupWithACommittedPositionStillReadsFromTheBeginningWhenPartitionsAreAssigned() {
        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);
        TopicPartition partition = new TopicPartition("news.search-queries", 0);

        // 재시작한 그룹은 읽은 위치가 커밋돼 있어도(여기서는 offset 5) 처음으로 돌아가 마지막 목록을 다시 받아야 한다.
        new SearchQueriesListener(received, JsonMapper.builder().build()).onPartitionsAssigned(Map.of(partition, 5L), callback);

        verify(callback).seekToBeginning(Set.of(partition));
    }
}

package dev.stockreading.news.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import dev.stockreading.news.event.HoldingsEnvelope;
import dev.stockreading.news.searchquery.GeneratedSearchQueries;
import tools.jackson.databind.json.JsonMapper;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. */
class HoldingsListenerTest {

    private final ReceivedHoldings holdings = new ReceivedHoldings();
    private final GeneratedSearchQueries generated = mock(GeneratedSearchQueries.class);
    private final HoldingsListener listener = new HoldingsListener(holdings, generated, JsonMapper.builder().build());

    @Test
    void beforeAnyListIsReceivedAnEtfHasNoMembers() {
        assertThat(holdings.membersOf("TEST01")).isEmpty();
    }

    @Test
    void theCommittedSampleBecomesTheMembersOfItsEtfAndGeneratesQueries() throws IOException {
        listener.onHoldings(List.of(ContractSchemas.sample("market.holdings.sample.json")));

        assertThat(holdings.membersOf("TEST01")).containsExactlyInAnyOrder("FAKE", "FAKE-B");
        assertThat(holdings.membersOf("TEST02")).isEmpty();
        verify(generated).generate(any());
    }

    @Test
    void theSameListTwiceGivesTheSameMembersAndANewListReplacesThePreviousOne() throws IOException {
        String sample = ContractSchemas.sample("market.holdings.sample.json");
        listener.onHoldings(List.of(sample));
        listener.onHoldings(List.of(sample));
        assertThat(holdings.membersOf("TEST01")).hasSize(2);

        listener.onHoldings(List.of(sample.replace("\"memberSymbol\": \"FAKE-B\"", "\"memberSymbol\": \"FAKE\"")));
        assertThat(holdings.membersOf("TEST01")).containsExactly("FAKE");
    }

    @Test
    void aBrokenEventKeepsThePreviousListAndGeneratesNothing() throws IOException {
        listener.onHoldings(List.of(ContractSchemas.sample("market.holdings.sample.json")));

        listener.onHoldings(List.of("{\"eventId\":\"x\",\"schemaVersion\":1,\"payload\":{\"contentHash\":\"h\"}}"));
        listener.onHoldings(List.of("{\"eventId\":\"x\",\"schemaVersion\":1}"));

        assertThat(holdings.membersOf("TEST01")).containsExactlyInAnyOrder("FAKE", "FAKE-B");
        verify(generated, org.mockito.Mockito.times(1)).generate(any());
    }

    @Test
    void aRestartBatchOfOldListsGeneratesQueriesOnlyFromTheLastOne() throws IOException {
        String sample = ContractSchemas.sample("market.holdings.sample.json");
        String older = sample.replace("\"memberSymbol\": \"FAKE-B\"", "\"memberSymbol\": \"FAKE-C\"");

        listener.onHoldings(List.of(older, sample, "{\"eventId\":\"x\",\"schemaVersion\":1}"));

        // 멤버십은 마지막 정상 목록이 이기고, 검색어 만들기는 그 목록으로 한 번만 돈다.
        assertThat(holdings.membersOf("TEST01")).containsExactlyInAnyOrder("FAKE", "FAKE-B");
        ArgumentCaptor<HoldingsEnvelope.HoldingsPayload> captor = ArgumentCaptor.forClass(HoldingsEnvelope.HoldingsPayload.class);
        verify(generated, org.mockito.Mockito.times(1)).generate(captor.capture());
        assertThat(captor.getValue().memberships()).extracting(HoldingsEnvelope.Membership::memberSymbol)
                .contains("FAKE-B").doesNotContain("FAKE-C");
    }

    @Test
    void anUnreadableMessageInABatchDoesNotDropTheValidListAfterIt() throws IOException {
        String sample = ContractSchemas.sample("market.holdings.sample.json");

        listener.onHoldings(List.of("not json", sample));

        assertThat(holdings.membersOf("TEST01")).containsExactlyInAnyOrder("FAKE", "FAKE-B");
        verify(generated, org.mockito.Mockito.times(1)).generate(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void onlyMembersOfDomesticEtfsAreKept() throws IOException {
        String sample = ContractSchemas.sample("market.holdings.sample.json").replace("\"etfMarket\": \"KR\"", "\"etfMarket\": \"US\"");

        listener.onHoldings(List.of(sample));

        assertThat(holdings.membersOf("TEST01")).isEmpty();
    }

    @Test
    void theListenerUsesItsOwnGroupAndReadsFromTheBeginningSoARestartGetsTheLastList() throws Exception {
        Method method = HoldingsListener.class.getDeclaredMethod("onHoldings", List.class);
        KafkaListener annotation = method.getAnnotation(KafkaListener.class);

        assertThat(annotation.topics()).containsExactly("market.holdings");
        assertThat(annotation.groupId()).isEqualTo("news-holdings");
        assertThat(annotation.properties()).contains("auto.offset.reset=earliest");

        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);
        Set<TopicPartition> partitions = Set.of(new TopicPartition("market.holdings", 0));
        listener.onPartitionsAssigned(Map.of(new TopicPartition("market.holdings", 0), 5L), callback);
        verify(callback).seekToBeginning(partitions);
    }
}

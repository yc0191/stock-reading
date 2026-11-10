package dev.stockreading.collector.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.json.JsonMapper;

class HoldingsListenerTest {

    private final ReceivedHoldings holdings = new ReceivedHoldings();
    private final HoldingsListener listener = new HoldingsListener(holdings, JsonMapper.builder().build());

    @Test
    void theCommittedSampleBecomesTheReceivedList() throws IOException {
        listener.onHoldings(ContractSchemas.sample("market.holdings.sample.json"));

        assertThat(holdings.allTickers()).containsExactly("FAKE", "FAKE-B");
    }

    @Test
    void aBrokenEventKeepsThePreviousList() throws IOException {
        listener.onHoldings(ContractSchemas.sample("market.holdings.sample.json"));

        listener.onHoldings("{\"eventId\":\"x\",\"schemaVersion\":1,\"payload\":{\"contentHash\":\"h\"}}");
        listener.onHoldings("{\"eventId\":\"x\",\"schemaVersion\":1}");

        assertThat(holdings.allTickers()).containsExactly("FAKE", "FAKE-B");
    }

    @Test
    void theListenerUsesItsOwnGroupAndReadsFromTheBeginningSoANewGroupGetsTheLastList() throws Exception {
        Method method = HoldingsListener.class.getDeclaredMethod("onHoldings", String.class);
        KafkaListener annotation = method.getAnnotation(KafkaListener.class);

        assertThat(annotation.topics()).containsExactly("market.holdings");
        assertThat(annotation.groupId()).isEqualTo("collector-holdings");
        assertThat(annotation.properties()).contains("auto.offset.reset=earliest");
    }

    @Test
    void aGroupWithACommittedPositionStillReadsFromTheBeginningWhenPartitionsAreAssigned() {
        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);
        TopicPartition partition = new TopicPartition("market.holdings", 0);

        // 재시작한 그룹은 읽은 위치가 커밋돼 있어도(여기서는 offset 5) 처음으로 돌아가 마지막 목록을 다시 받아야 한다.
        listener.onPartitionsAssigned(Map.of(partition, 5L), callback);

        verify(callback).seekToBeginning(Set.of(partition));
    }
}

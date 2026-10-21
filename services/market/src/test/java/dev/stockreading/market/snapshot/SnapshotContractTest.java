package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class SnapshotContractTest {

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void v1SampleMatchesSchema() throws IOException {
        var errors = ContractSchemas.validate("market.snapshot.v1.schema.json", ContractSchemas.sample("market.snapshot.sample.json"));
        assertThat(errors).isEmpty();
    }

    @Test
    void v1SampleIsReadableByCurrentConsumer() throws IOException {
        SnapshotEnvelope event = mapper.readValue(ContractSchemas.sample("market.snapshot.sample.json"), SnapshotEnvelope.class);

        assertThat(event.eventType()).isEqualTo("market.snapshot");
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
        assertThat(event.payload().price()).isEqualTo(10_250L);
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("market.snapshot.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", withExtraFields)).isEmpty();
        SnapshotEnvelope event = mapper.readValue(withExtraFields, SnapshotEnvelope.class);
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
    }
}

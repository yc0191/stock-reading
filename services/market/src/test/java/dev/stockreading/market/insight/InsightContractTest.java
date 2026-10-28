package dev.stockreading.market.insight;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class InsightContractTest {

    private static final String SCHEMA = "market.insight.v1.schema.json";
    private static final String SAMPLE = "market.insight.sample.json";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void v1SampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, ContractSchemas.sample(SAMPLE))).isEmpty();
    }

    @Test
    void v1SampleIsReadableByCurrentConsumer() throws IOException {
        InsightEnvelope event = mapper.readValue(ContractSchemas.sample(SAMPLE), InsightEnvelope.class);

        assertThat(event.eventType()).isEqualTo("market.insight");
        assertThat(event.payload().apiType()).isEqualTo("daily-price");
        assertThat(event.payload().target()).isEqualTo("TEST01");
        assertThat(event.payload().response()).contains("\"close\": \" 10000 \"");
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample(SAMPLE)
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate(SCHEMA, withExtraFields)).isEmpty();
        InsightEnvelope event = mapper.readValue(withExtraFields, InsightEnvelope.class);
        assertThat(event.payload().target()).isEqualTo("TEST01");
    }

    @Test
    void everyApiTypeInTheSchemaHasATable() throws IOException {
        var schema = mapper.readTree(java.nio.file.Files.readString(java.nio.file.Path.of("../../contracts/events", SCHEMA)));
        var values = schema.at("/properties/payload/properties/apiType/enum");

        assertThat(values).isNotEmpty();
        for (var value : values) {
            assertThat(ApiType.fromWire(value.asString()).table()).isEqualTo(value.asString().replace('-', '_'));
        }
        assertThat(values.size()).isEqualTo(ApiType.values().length);
    }
}

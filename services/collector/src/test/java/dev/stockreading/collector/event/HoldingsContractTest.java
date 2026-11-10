package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 시세처리기가 내는 보유 종목 목록을 수집기가 계속 읽을 수 있어야 한다. */
class HoldingsContractTest {

    private static final String SCHEMA = "market.holdings.v1.schema.json";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void committedSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("market.holdings.sample.json");

        assertThat(ContractSchemas.validate(SCHEMA, sample)).isEmpty();
        HoldingsEnvelope event = mapper.readValue(sample, HoldingsEnvelope.class);
        assertThat(event.eventType()).isEqualTo("market.holdings");
        assertThat(event.payload().instruments()).hasSize(4);
        assertThat(event.payload().instruments().get(0).symbol()).isEqualTo("TEST01");
        assertThat(event.payload().memberships()).hasSize(2);
        assertThat(event.payload().memberships().get(1).memberSymbol()).isEqualTo("FAKE-B");
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersionsIncludingTheOptionalName() throws IOException {
        String json = ContractSchemas.sample("market.holdings.sample.json")
                .replace("\"contentHash\"", "\"futurePayloadField\": {\"a\": 1},\n    \"contentHash\"");

        assertThat(json).contains("futurePayloadField", "\"name\"");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        assertThat(mapper.readValue(json, HoldingsEnvelope.class).payload().instruments()).hasSize(4);
    }
}

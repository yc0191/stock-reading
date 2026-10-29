package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

class FilingCollectedContractTest {

    private static final String SCHEMA = "filing.collected.v1.schema.json";

    @Test
    void committedSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample())).isEmpty();
    }

    @Test
    void sixKWithoutItemsAndClassTickerAreAllowed() throws IOException {
        String json = sample()
                .replace("\"items\": [\"2.02\", \"9.01\"]", "\"items\": []")
                .replace("\"form\": \"8-K\"", "\"form\": \"6-K\"")
                .replace("[\"TESTA\"]", "[\"TESTA\", \"TESTA.B\", \"TESTB-C\"]");

        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void unknownFieldsAreIgnoredForCompatibility() throws IOException {
        String json = sample()
                .replace("\"correlationId\"", "\"futureEnvelopeField\": 1,\n  \"correlationId\"")
                .replace("\"cik\"", "\"futurePayloadField\": {\"a\": 1},\n    \"cik\"");

        assertThat(json).contains("futureEnvelopeField", "futurePayloadField");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void malformedIdentifiersAreRejected() throws IOException {
        assertRejected(sample().replace("\"cik\": \"0000000001\"", "\"cik\": \"1\""));
        assertRejected(sample().replace("\"0000000001-26-000001\"", "\"000000000126000001\""));
        assertRejected(sample().replace("[\"TESTA\"]", "[\"testa\"]"));
        assertRejected(sample().replace("[\"TESTA\"]", "[]"));
        assertRejected(sample().replace("[\"TESTA\"]", "[\"TESTA\", \"TESTA\"]"));
    }

    @Test
    void malformedTimesAndUrlAreRejected() throws IOException {
        assertRejected(sample().replace("\"acceptedAt\": \"2026-10-20T20:30:28Z\"", "\"acceptedAt\": \"2026-10-20 20:30\""));
        assertRejected(sample().replace("\"filingDate\": \"2026-10-20\"", "\"filingDate\": \"20261020\""));
        assertRejected(sample().replace("https://www.sec.gov/Archives/edgar/data/1/000000000126000001/test-8k.htm",
                "not a url"));
    }

    @Test
    void missingRequiredPayloadFieldsAreRejected() throws IOException {
        assertRejected(sample().replace("    \"items\": [\"2.02\", \"9.01\"],\n", ""));
        assertRejected(sample().replace("    \"form\": \"8-K\",\n", ""));
        assertRejected(sample().replace("    \"acceptedAt\": \"2026-10-20T20:30:28Z\",\n", ""));
    }

    @Test
    void wrongEventTypeIsRejected() throws IOException {
        assertRejected(sample().replace("\"filing.collected\"", "\"news.collected\""));
    }

    @Test
    void scheduleTickSchemaAcceptsThePreOpenJobType() throws IOException {
        String tick = ContractSchemas.sample("schedule.tick.sample.json").replace("\"collect\"", "\"collect-pre-open\"");

        assertThat(tick).contains("collect-pre-open");
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", tick)).isEmpty();
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("filing.collected.sample.json");
    }

    // 치환이 실제로 일어났는지 먼저 확인해야, 음성 테스트가 원본 샘플을 검증하며 통과하는 일이 없다.
    private static void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

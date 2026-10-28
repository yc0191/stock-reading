package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

class CollectionCompletedContractTest {

    private static final String SCHEMA = "collection.completed.v1.schema.json";

    @Test
    void committedSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample())).isEmpty();
    }

    @Test
    void emptyIncompleteSymbolsAndMarketWideSymbolAreAllowed() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample().replace("[\"TEST02\"]", "[]"))).isEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample().replace("[\"TEST02\"]", "[\"*\", \"TEST02\"]"))).isEmpty();
    }

    @Test
    void unknownFieldsAreIgnoredForCompatibility() throws IOException {
        String json = sample()
                .replace("\"correlationId\"", "\"futureEnvelopeField\": 1,\n  \"correlationId\"")
                .replace("\"jobType\"", "\"futurePayloadField\": {\"a\": 1},\n    \"jobType\"");

        assertThat(json).contains("futureEnvelopeField", "futurePayloadField");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void invalidIncompleteSymbolsAreRejected() throws IOException {
        assertRejected(sample().replace("[\"TEST02\"]", "[\"test02\"]"));
        assertRejected(sample().replace("[\"TEST02\"]", "[\"TEST02\", \"TEST02\"]"));
    }

    @Test
    void missingRequiredPayloadFieldIsRejected() throws IOException {
        assertRejected(sample().replace("    \"startedAt\": \"2026-10-20T16:15:01+09:00\",\n", ""));
        assertRejected(sample().replace("    \"incompleteSymbols\": [\"TEST02\"]\n", "    \"publishedCount\": 12\n")
                .replace("    \"publishedCount\": 12,\n", ""));
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("collection.completed.sample.json");
    }

    private static void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

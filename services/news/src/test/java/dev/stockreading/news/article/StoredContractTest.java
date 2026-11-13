package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** news.stored 를 내는 쪽과 collection.completed 를 받는 쪽의 계약 검사. */
class StoredContractTest {

    private static final String SCHEMA = "news.stored.v1.schema.json";
    private static final String SAMPLE = "news.stored.sample.json";

    @Test
    void v1SampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, ContractSchemas.sample(SAMPLE))).isEmpty();
    }

    // 샘플의 eventId 는 실제로 그 수집 완료에 대해 내는 값이다.
    @Test
    void sampleEventIdIsDerivedFromTheCollectionEventId() throws IOException {
        JsonNode sample = JsonMapper.builder().build().readTree(ContractSchemas.sample(SAMPLE));

        assertThat(sample.get("eventId").asString()).isEqualTo(
                StoredNotifier.storedEventId(sample.get("payload").get("collectionEventId").asString()).toString());
    }

    @Test
    void countsMustBeNonNegativeAndArticleIdsUuids() throws IOException {
        String sample = ContractSchemas.sample(SAMPLE);

        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("\"failedCount\": 0", "\"failedCount\": -1")))
                .isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("e0000012-0000-4000-8000-000000000012", "x")))
                .isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("    \"mergedCount\": 2,\n", ""))).isNotEmpty();
    }

    @Test
    void collectionCompletedSampleIsReadByThisConsumer() throws IOException {
        CollectionCompletedEnvelope event = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample("collection.completed.sample.json"), CollectionCompletedEnvelope.class);

        assertThat(event.payload().articleEventIds()).hasSize(event.payload().publishedCount());
        assertThat(event.payload().incompleteSymbols()).containsExactly("TEST02");
    }
}

package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class ArticlesContractTest {

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("news.articles.requested.sample.json");

        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json", sample)).isEmpty();
        ArticlesRequestEnvelope event = mapper.readValue(sample, ArticlesRequestEnvelope.class);
        assertThat(event.eventType()).isEqualTo("news.articles.requested");
        assertThat(event.payload().jobId()).isEqualTo("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");
        assertThat(event.payload().seq()).isEqualTo(1);
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
        assertThat(event.payload().maxArticles()).isEqualTo(300);
        assertThat(event.payload().researchJobId()).isNull();
    }

    @Test
    void researchJobIdIsReadWhenPresentAndMustBeAUuid() throws IOException {
        String sample = ContractSchemas.sample("news.articles.requested.sample.json");
        String withResearch = sample.replace(
                "\"maxArticles\": 300", "\"maxArticles\": 300,\n    \"researchJobId\": \"5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"");

        assertThat(withResearch).isNotEqualTo(sample);
        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json", withResearch)).isEmpty();
        assertThat(mapper.readValue(withResearch, ArticlesRequestEnvelope.class).payload().researchJobId())
                .isEqualTo("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");
        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json",
                withResearch.replace("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"\n", "not-a-uuid\"\n"))).isNotEmpty();
    }

    @Test
    void responseSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate("news.articles.responded.v1.schema.json",
                ContractSchemas.sample("news.articles.responded.sample.json"))).isEmpty();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json", withExtraFields)).isEmpty();
        assertThat(mapper.readValue(withExtraFields, ArticlesRequestEnvelope.class).payload().symbol())
                .isEqualTo("TEST01");
    }
}

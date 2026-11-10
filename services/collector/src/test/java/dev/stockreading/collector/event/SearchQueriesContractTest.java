package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 뉴스 서비스가 내는 검색어 목록을 수집기가 계속 읽을 수 있어야 한다. */
class SearchQueriesContractTest {

    private static final String SCHEMA = "news.search-queries.v1.schema.json";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void committedSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("news.search-queries.sample.json");

        assertThat(ContractSchemas.validate(SCHEMA, sample)).isEmpty();
        SearchQueriesEnvelope event = mapper.readValue(sample, SearchQueriesEnvelope.class);
        assertThat(event.eventType()).isEqualTo("news.search-queries");
        assertThat(event.payload().queries()).extracting(SearchQueriesEnvelope.Query::query)
                .containsExactly("테스트 ETF 하나", "Fake Corp", "Fake Corp stock", "가짜 시장 이슈");
        assertThat(event.payload().queries().get(2).symbols()).containsExactly("FAKE", "FAKE-B");
        assertThat(event.payload().queries().get(3).symbols()).containsExactly("*");
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String json = ContractSchemas.sample("news.search-queries.sample.json")
                .replace("\"contentHash\"", "\"futurePayloadField\": {\"a\": 1},\n    \"contentHash\"");

        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        assertThat(mapper.readValue(json, SearchQueriesEnvelope.class).payload().queries()).hasSize(4);
    }

    @Test
    void schemaRejectsABadSymbolAnEmptySymbolListAndAnEmptyQuery() throws IOException {
        String sample = ContractSchemas.sample("news.search-queries.sample.json");

        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("\"FAKE-B\"", "\"fake\""))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("[\"*\"]", "[]"))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("\"가짜 시장 이슈\"", "\"\""))).isNotEmpty();
    }
}

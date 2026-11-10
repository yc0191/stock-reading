package dev.stockreading.news.searchquery;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.Envelope;
import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.event.SearchQueriesPayload;
import dev.stockreading.news.event.SearchQueriesPayload.Query;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 계약 샘플과 실제로 만드는 이벤트가 스키마를 지키는지 확인한다(ADR-003/010). */
class SearchQueriesContractTest {

    private static final String SCHEMA = "news.search-queries.v1.schema.json";
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-10T01:00:30Z"), ZoneId.of("Asia/Seoul"));

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void sampleMatchesTheSchemaAndCoversDomesticUsAndMarketWideTags() throws IOException {
        String sample = ContractSchemas.sample("news.search-queries.sample.json");

        assertThat(ContractSchemas.validate(SCHEMA, sample)).isEmpty();
        assertThat(mapper.readTree(sample).get("payload").get("queries")).hasSize(4);
    }

    @Test
    void theEventTheProducerBuildsMatchesTheSchemaIncludingAnEmptyList() throws IOException {
        for (List<Query> queries : List.of(
                List.of(new Query("가짜 검색어", List.of("TEST01", "FAKE-B")), new Query("가짜 시황", List.of("*"))),
                List.<Query>of())) {
            Envelope<SearchQueriesPayload> event = EventFactory.create(CLOCK, Topics.NEWS_SEARCH_QUERY_LIST,
                    OffsetDateTime.now(CLOCK), "search-queries-change-test",
                    new SearchQuerySnapshot(queries).toPayload());

            String json = mapper.writeValueAsString(event);

            assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
            JsonNode root = mapper.readTree(json);
            assertThat(root.get("eventType").asString()).isEqualTo("news.search-queries");
            assertThat(root.get("payload").get("queries")).hasSize(queries.size());
        }
    }

    @Test
    void schemaRejectsABadHashABadSymbolAnEmptySymbolListAndATooLongQuery() throws IOException {
        String sample = ContractSchemas.sample("news.search-queries.sample.json");

        assertThat(ContractSchemas.validate(SCHEMA, sample.replaceFirst("\"contentHash\": \"[0-9a-f]+\"",
                "\"contentHash\": \"XYZ\""))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("\"FAKE-B\"", "\"fake\""))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("[\"*\"]", "[]"))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("\"가짜 시장 이슈\"",
                "\"" + "가".repeat(201) + "\""))).isNotEmpty();
    }

    @Test
    void consumersIgnoreFieldsAddedInFutureVersions() throws IOException {
        String future = ContractSchemas.sample("news.search-queries.sample.json")
                .replace("\"contentHash\"", "\"newField\": 1, \"contentHash\"");

        assertThat(ContractSchemas.validate(SCHEMA, future)).isEmpty();
    }
}

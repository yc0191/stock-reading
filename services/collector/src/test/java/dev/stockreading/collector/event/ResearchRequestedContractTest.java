package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 브리핑이 보내는 재검색 요청을 수집기가 계속 읽을 수 있어야 한다. */
class ResearchRequestedContractTest {

    private static final String SCHEMA = "news.research.requested.v1.schema.json";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void committedSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample())).isEmpty();

        ResearchRequestEnvelope event = mapper.readValue(sample(), ResearchRequestEnvelope.class);
        assertThat(event.eventId()).isEqualTo("3e7b1c90-5a24-4d68-b0f3-8c1d2e4a6b57");
        assertThat(event.eventType()).isEqualTo("news.research.requested");
        assertThat(event.payload().jobId()).isEqualTo("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");
        assertThat(event.payload().seq()).isEqualTo(1);
        assertThat(event.payload().jobKind()).isEqualTo("move");
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
        assertThat(event.payload().queries()).containsExactly("테스트 재검색어 하나", "테스트 재검색어 둘");
    }

    @Test
    void dailyEditionsWithMarketWideTagAreAllowed() throws IOException {
        for (String kind : new String[] {"morning", "post-close"}) {
            String json = sample()
                    .replace("\"jobKind\": \"move\"", "\"jobKind\": \"" + kind + "\"")
                    .replace("\"symbol\": \"TEST01\"", "\"symbol\": \"*\"");

            assertThat(ContractSchemas.validate(SCHEMA, json)).as(kind).isEmpty();
        }
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String json = sample()
                .replace("\"correlationId\"", "\"futureEnvelopeField\": 1,\n  \"correlationId\"")
                .replace("\"jobKind\"", "\"futurePayloadField\": {\"a\": 1},\n    \"jobKind\"");

        assertThat(json).contains("futureEnvelopeField", "futurePayloadField");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        assertThat(mapper.readValue(json, ResearchRequestEnvelope.class).payload().symbol()).isEqualTo("TEST01");
    }

    @Test
    void unknownJobKindIsRejected() throws IOException {
        assertRejected(sample().replace("\"jobKind\": \"move\"", "\"jobKind\": \"noon\""));
    }

    @Test
    void malformedSymbolIsRejected() throws IOException {
        assertRejected(sample().replace("\"symbol\": \"TEST01\"", "\"symbol\": \"test01\""));
        assertRejected(sample().replace("\"symbol\": \"TEST01\"", "\"symbol\": \"TEST011\""));
    }

    @Test
    void emptyDuplicateOrTooManyQueriesAreRejected() throws IOException {
        String queries = "[\"테스트 재검색어 하나\", \"테스트 재검색어 둘\"]";
        assertRejected(sample().replace(queries, "[]"));
        assertRejected(sample().replace(queries, "[\"테스트 재검색어 하나\", \"테스트 재검색어 하나\"]"));
        assertRejected(sample().replace(queries, "[\"테스트1\", \"테스트2\", \"테스트3\", \"테스트4\"]"));
        assertRejected(sample().replace(queries, "[\"\"]"));
    }

    @Test
    void missingRequiredPayloadFieldsAreRejected() throws IOException {
        assertRejected(sample().replace("    \"seq\": 1,\n", ""));
        assertRejected(sample().replace("    \"jobKind\": \"move\",\n", ""));
        assertRejected(sample().replace("    \"symbol\": \"TEST01\",\n", ""));
        assertRejected(sample().replace(",\n    \"queries\": [\"테스트 재검색어 하나\", \"테스트 재검색어 둘\"]", ""));
    }

    @Test
    void malformedJobIdIsRejected() throws IOException {
        assertRejected(sample().replace("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13", "not-a-uuid"));
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("news.research.requested.sample.json");
    }

    // 치환이 실제로 일어났는지 먼저 확인해야, 음성 테스트가 원본 샘플을 검증하며 통과하는 일이 없다.
    private void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

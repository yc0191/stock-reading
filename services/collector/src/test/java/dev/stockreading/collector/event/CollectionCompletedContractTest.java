package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

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

    // 기사 이벤트 id 목록은 뒤에 더한 칸이라, 이전 수집기가 낸 완료(칸 없음)도 계약을 지킨다.
    @Test
    void articleEventIdsAreOptionalUniqueUuids() throws IOException {
        String withoutIds = sample().replaceAll("(?s)    \"articleEventIds\": \\[.*?\\],\n", "");
        assertThat(withoutIds).doesNotContain("articleEventIds");
        assertThat(ContractSchemas.validate(SCHEMA, withoutIds)).isEmpty();

        assertRejected(sample().replace("e0000001-0000-4000-8000-000000000001", "not-a-uuid"));
        assertRejected(sample().replace("e0000002-0000-4000-8000-000000000002", "e0000001-0000-4000-8000-000000000001"));
    }

    @Test
    void researchCompletionCarriesItsJobIdAsAnOptionalUuid() throws IOException {
        String research = sample()
                .replace("\"jobType\": \"collect-news\"", "\"jobType\": \"research-news\"")
                .replace("\"incompleteSymbols\": [\"TEST02\"]",
                        "\"incompleteSymbols\": [\"TEST02\"],\n    \"researchJobId\": \"5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"");

        assertThat(research).contains("researchJobId", "research-news");
        assertThat(ContractSchemas.validate(SCHEMA, research)).isEmpty();
        assertRejected(research.replace("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13", "not-a-uuid"));
    }

    @Test
    void producedCompletionCarriesResearchJobIdOnlyWhenPresent() throws IOException {
        ObjectMapper mapper = JsonMapper.builder().build();
        Clock clock = Clock.fixed(Instant.parse("2026-10-20T07:15:42Z"), ZoneId.of("Asia/Seoul"));
        OffsetDateTime at = OffsetDateTime.now(clock);
        String time = EventFactory.format(at);
        CollectionCompletedPayload regular = new CollectionCompletedPayload(
                "collect-news", time, time, time, 0, List.of(), List.of(), null);
        CollectionCompletedPayload research = new CollectionCompletedPayload(
                "research-news", time, time, time, 0, List.of(), List.of("TEST01"), "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");

        String regularJson = mapper.writeValueAsString(
                EventFactory.create(clock, "collection.completed", at, "c", regular));
        String researchJson = mapper.writeValueAsString(
                EventFactory.create(clock, "collection.completed", at, "c", research));

        assertThat(regularJson).doesNotContain("researchJobId");
        assertThat(ContractSchemas.validate(SCHEMA, regularJson)).isEmpty();
        assertThat(researchJson).contains("\"researchJobId\":\"5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"");
        assertThat(ContractSchemas.validate(SCHEMA, researchJson)).isEmpty();
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("collection.completed.sample.json");
    }

    private static void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

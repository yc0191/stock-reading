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

class NewsCollectedContractTest {

    private static final String SCHEMA = "news.collected.v1.schema.json";

    @Test
    void committedSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, ContractSchemas.sample("news.collected.sample.json"))).isEmpty();
    }

    @Test
    void marketWideSymbolAndEmptySummaryAreAllowed() throws IOException {
        String json = sample()
                .replace("\"symbols\": [\"TEST01\"]", "\"symbols\": [\"*\", \"TEST01\"]")
                .replace("\"summary\": \"테스트 기사 요약입니다.\"", "\"summary\": \"\"");

        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void matchedQueriesIsOptional() throws IOException {
        String json = sample().replace(",\n    \"matchedQueries\": [\"테스트검색어\"]", "");

        assertThat(json).doesNotContain("matchedQueries");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void unknownFieldsAreIgnoredForCompatibility() throws IOException {
        String json = sample()
                .replace("\"correlationId\"", "\"futureEnvelopeField\": 1,\n  \"correlationId\"")
                .replace("\"publisher\"", "\"futurePayloadField\": {\"a\": 1},\n    \"publisher\"");

        assertThat(json).contains("futureEnvelopeField", "futurePayloadField");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void symbolWithSevenCharactersIsRejected() throws IOException {
        assertRejected(sample().replace("\"TEST01\"", "\"TEST011\""));
    }

    @Test
    void lowercaseOrShortSymbolIsRejected() throws IOException {
        assertRejected(sample().replace("\"TEST01\"", "\"test01\""));
        assertRejected(sample().replace("\"TEST01\"", "\"TEST1\""));
    }

    @Test
    void emptySymbolsIsRejected() throws IOException {
        assertRejected(sample().replace("[\"TEST01\"]", "[]"));
    }

    @Test
    void duplicateSymbolsAreRejected() throws IOException {
        assertRejected(sample().replace("[\"TEST01\"]", "[\"TEST01\", \"TEST01\"]"));
    }

    @Test
    void duplicateMatchedQueriesAreRejected() throws IOException {
        assertRejected(sample().replace("[\"테스트검색어\"]", "[\"테스트검색어\", \"테스트검색어\"]"));
    }

    @Test
    void malformedPublishedAtIsRejected() throws IOException {
        assertRejected(sample().replace(
                "\"publishedAt\": \"2026-10-20T15:42:00+09:00\"", "\"publishedAt\": \"2026-10-20 15:42\""));
    }

    @Test
    void malformedUrlIsRejected() throws IOException {
        assertRejected(sample().replace("https://news.example.com/articles/12345", "not a url"));
    }

    @Test
    void titleOutOfRangeIsRejected() throws IOException {
        assertRejected(sample().replace("\"title\": \"테스트 기사 제목\"", "\"title\": \"\""));
        assertRejected(sample().replace("\"title\": \"테스트 기사 제목\"", "\"title\": \"" + "가".repeat(501) + "\""));
    }

    @Test
    void summaryOverLimitIsRejected() throws IOException {
        assertRejected(sample().replace("테스트 기사 요약입니다.", "가".repeat(2001)));
    }

    @Test
    void missingRequiredPayloadFieldsAreRejected() throws IOException {
        assertRejected(sample().replace("    \"url\": \"https://news.example.com/articles/12345\",\n", ""));
        assertRejected(sample().replace("    \"publisher\": \"news.example.com\",\n", ""));
        assertRejected(sample().replace("    \"summary\": \"테스트 기사 요약입니다.\",\n", ""));
        assertRejected(sample().replace("    \"symbols\": [\"TEST01\"],\n", ""));
    }

    @Test
    void missingEnvelopeFieldIsRejected() throws IOException {
        assertRejected(sample().replace("  \"correlationId\": \"collect-news-2026-10-20T16:00:00+09:00\",\n", ""));
    }

    @Test
    void wrongEventTypeIsRejected() throws IOException {
        assertRejected(sample().replace("\"news.collected\"", "\"market.snapshot\""));
    }

    @Test
    void scheduleTickSchemaStillAcceptsTheNewsJobType() throws IOException {
        String tick = ContractSchemas.sample("schedule.tick.sample.json").replace("\"collect\"", "\"collect-news\"");

        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", tick)).isEmpty();
    }

    @Test
    void researchJobIdIsOptionalAndMustBeAUuid() throws IOException {
        String withResearch = sample().replace(
                "\"symbols\": [\"TEST01\"]",
                "\"symbols\": [\"TEST01\"],\n    \"researchJobId\": \"5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"");

        assertThat(withResearch).contains("researchJobId");
        assertThat(ContractSchemas.validate(SCHEMA, withResearch)).isEmpty();
        assertRejected(withResearch.replace("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13", "not-a-uuid"));
    }

    @Test
    void producedPayloadCarriesResearchJobIdOnlyWhenPresent() throws IOException {
        ObjectMapper mapper = JsonMapper.builder().build();
        Clock clock = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
        OffsetDateTime at = OffsetDateTime.now(clock);
        String time = EventFactory.format(at);
        NewsPayload regular = new NewsPayload("테스트 제목", "", "https://news.example.com/a/1", "news.example.com",
                time, List.of("TEST01"), List.of("테스트검색어"));
        NewsPayload research = new NewsPayload("테스트 제목", "", "https://news.example.com/a/1", "news.example.com",
                time, List.of("TEST01"), List.of("테스트검색어"), "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");

        String regularJson = mapper.writeValueAsString(EventFactory.create(clock, "news.collected", at, "c", regular));
        String researchJson = mapper.writeValueAsString(EventFactory.create(clock, "news.collected", at, "c", research));

        assertThat(regularJson).doesNotContain("researchJobId");
        assertThat(ContractSchemas.validate(SCHEMA, regularJson)).isEmpty();
        assertThat(researchJson).contains("\"researchJobId\":\"5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"");
        assertThat(ContractSchemas.validate(SCHEMA, researchJson)).isEmpty();
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("news.collected.sample.json");
    }

    // 치환이 실제로 일어났는지 먼저 확인해야, 음성 테스트가 원본 샘플을 검증하며 통과하는 일이 없다.
    private static void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

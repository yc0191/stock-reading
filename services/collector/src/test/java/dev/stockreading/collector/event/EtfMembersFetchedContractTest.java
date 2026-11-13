package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

/** 수집기가 만드는 etf.members.fetched 의 샘플이 계약을 지키고, 계약이 성공·실패 모양을 가려내는지 본다(ADR-003/010). */
class EtfMembersFetchedContractTest {

    private static final String SCHEMA = "etf.members.fetched.v1.schema.json";

    @Test
    void committedSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample())).isEmpty();
    }

    @Test
    void aFailureCarriesTheRawWithoutMembers() throws IOException {
        String failed = sample().replaceAll("(?s)\"members\": \\[.*?\\],", "\"members\": null,")
                .replace("\"failure\": null", "\"failure\": \"unexpected status 503\"")
                .replace("\"httpStatus\": 200", "\"httpStatus\": 503");

        assertThat(failed).contains("\"members\": null");
        assertThat(ContractSchemas.validate(SCHEMA, failed)).isEmpty();
        // 빈 원문도 원문이다(200이 아닌 응답은 본문이 없을 수 있다).
        assertThat(ContractSchemas.validate(SCHEMA,
                failed.replaceAll("\"response\": \".*?\\\\n\"", "\"response\": \"\""))).isEmpty();
    }

    @Test
    void successWithoutMembersFailureWithMembersAndBadValuesAreRejected() throws IOException {
        assertRejected(sample().replaceAll("(?s)\"members\": \\[.*?\\],", "\"members\": null,"));
        assertRejected(sample().replace("\"failure\": null", "\"failure\": \"unexpected shape\""));
        assertRejected(sample().replaceAll("(?s)\"members\": \\[.*?\\],", "\"members\": [],"));
        assertRejected(sample().replace("\"etfSymbol\": \"TEST01\"", "\"etfSymbol\": \"TEST1\""));
        assertRejected(sample().replace("\"baseDate\": \"2026-11-06\"", "\"baseDate\": \"2026.11.06\""));
        assertRejected(sample().replace("\"weight\": \"9.50\"", "\"weight\": \" 9.50 \""));
        assertRejected(sample().replace("\"market\": \"US\"", "\"market\": \"JP\""));
        assertRejected(sample().replace("\"rank\": 1", "\"rank\": 0"));
        assertRejected(sample().replace("\"eventType\": \"etf.members.fetched\"", "\"eventType\": \"schedule.tick\""));
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("etf.members.fetched.sample.json");
    }

    // 치환이 실제로 일어났는지 먼저 확인해야, 음성 테스트가 원본 샘플을 검증하며 통과하는 일이 없다.
    private void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

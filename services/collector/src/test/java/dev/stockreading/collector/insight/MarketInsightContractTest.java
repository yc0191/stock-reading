package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dev.stockreading.collector.event.ContractSchemas;

class MarketInsightContractTest {

    private static final String SCHEMA = "market.insight.v1.schema.json";

    @Test
    void committedSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample())).isEmpty();
    }

    @Test
    void nullTargetIsAllowedForSummaries() throws IOException {
        String json = sample()
                .replace("\"daily-price\"", "\"market-summary\"")
                .replace("\"target\": \"TEST01\"", "\"target\": null");

        assertThat(json).contains("\"target\": null");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void summaryWithTargetIsRejected() throws IOException {
        assertRejected(sample().replace("\"daily-price\"", "\"exchange-rate-summary\""));
    }

    @Test
    void dailyPriceWithoutSixCharacterTargetIsRejected() throws IOException {
        assertRejected(sample().replace("\"TEST01\"", "\"TEST011\""));
        assertRejected(sample().replace("\"TEST01\"", "\"test01\""));
        assertRejected(sample().replace("\"target\": \"TEST01\"", "\"target\": null"));
    }

    @Test
    void worldIndexNeedsNonEmptyTarget() throws IOException {
        String worldIndex = sample().replace("\"daily-price\"", "\"world-index\"");

        assertThat(ContractSchemas.validate(SCHEMA, worldIndex.replace("\"TEST01\"", "\"C\""))).isEmpty();
        assertRejected(worldIndex.replace("\"TEST01\"", "\"\""));
    }

    @Test
    void overseasDailyPriceNeedsAUsTickerTarget() throws IOException {
        String overseas = sample().replace("\"daily-price\"", "\"overseas-daily-price\"");

        assertThat(ContractSchemas.validate(SCHEMA, overseas.replace("\"TEST01\"", "\"TESTA\""))).isEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, overseas.replace("\"TEST01\"", "\"TESTB.X\""))).isEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, overseas.replace("\"TEST01\"", "\"TESTB-X\""))).isEmpty();
        assertRejected(overseas.replace("\"TEST01\"", "\"testa\""));
        assertRejected(overseas.replace("\"TEST01\"", "\"TESTABC\""));
        assertRejected(overseas.replace("\"TEST01\"", "\"\""));
        assertRejected(overseas.replace("\"target\": \"TEST01\"", "\"target\": null"));
    }

    @Test
    void unknownApiTypeIsRejected() throws IOException {
        assertRejected(sample().replace("\"daily-price\"", "\"order-history\""));
    }

    @Test
    void emptyOrObjectResponseIsRejected() throws IOException {
        assertRejected(sample().replaceAll("\"response\": \".*\",\n", "\"response\": \"\",\n"));
        assertRejected(sample().replaceAll("\"response\": \".*\",\n", "\"response\": {\"a\": 1},\n"));
    }

    @Test
    void missingPayloadFieldsAreRejected() throws IOException {
        assertRejected(sample().replace("    \"target\": \"TEST01\",\n", ""));
        assertRejected(sample().replace("    \"scheduledFor\": \"2026-10-20T16:30:00+09:00\",\n", ""));
        assertRejected(sample().replace(",\n    \"fetchedAt\": \"2026-10-20T16:30:01.800+09:00\"", ""));
    }

    @Test
    void malformedFetchedAtIsRejected() throws IOException {
        assertRejected(sample().replace("\"fetchedAt\": \"2026-10-20T16:30:01.800+09:00\"", "\"fetchedAt\": \"2026-10-20 16:30\""));
    }

    @Test
    void wrongEventTypeIsRejected() throws IOException {
        assertRejected(sample().replace("\"market.insight\"", "\"market.snapshot\""));
    }

    @Test
    void scheduleTickSchemaAcceptsThePostCloseJobType() throws IOException {
        String tick = ContractSchemas.sample("schedule.tick.sample.json").replace("\"collect\"", "\"collect-post-close\"");

        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", tick)).isEmpty();
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("market.insight.sample.json");
    }

    // 치환이 실제로 일어났는지 먼저 확인해야 음성 테스트가 원본을 검증하며 통과하는 일이 없다.
    private static void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

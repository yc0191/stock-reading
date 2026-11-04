package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.EventFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class DailyContractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T08:00:01Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "8d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("market.daily.requested.sample.json");

        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", sample)).isEmpty();
        DailyRequestEnvelope event = mapper.readValue(sample, DailyRequestEnvelope.class);
        assertThat(event.eventType()).isEqualTo("market.daily.requested");
        assertThat(event.payload().jobId()).isEqualTo(JOB_ID);
        assertThat(event.payload().seq()).isEqualTo(1);
        assertThat(event.payload().asOf()).isEqualTo("2026-10-20T17:00:00+09:00");
        assertThat(event.payload().since()).isEqualTo("2026-10-16T17:00:00+09:00");
        assertThat(event.payload().maxResponseBytes()).isEqualTo(32_768);
        assertThat(event.payload().maxEtfs()).isEqualTo(50);
        assertThat(event.payload().maxTickers()).isEqualTo(100);
    }

    @Test
    void responseSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate("market.daily.responded.v1.schema.json",
                ContractSchemas.sample("market.daily.responded.sample.json"))).isEmpty();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("market.daily.requested.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", withExtraFields)).isEmpty();
        assertThat(mapper.readValue(withExtraFields, DailyRequestEnvelope.class).payload().maxEtfs()).isEqualTo(50);
    }

    @Test
    void producerPayloadWithEtfsCloseRatesAndEveryBackgroundStatusMatchesSchema() throws IOException {
        DailyPayload payload = new DailyPayload(JOB_ID, 2,
                List.of(new DailyPayload.Etf("TEST01", "20261020", "9850", "10000", "-1.50",
                                "2026-10-20T16:30:02+09:00", List.of("TESTA", "FAKEB"), true),
                        new DailyPayload.Etf("TEST02", "20261020", "20100", "20000", "0.50",
                                "2026-10-20T16:30:03+09:00", List.of(), false)),
                List.of(new ContextPayload.CloseRate("TESTA", "20261019", "2026-10-20T05:00:00+09:00", "-1.25",
                        "2026-10-20T08:30:02+09:00")),
                List.of(
                        ContextPayload.Background.ok("exchange-rate-summary", null, "{\"a\": 1}",
                                "2026-10-20T16:30:05+09:00"),
                        ContextPayload.Background.tooLarge("market-summary", null, "2026-10-20T16:30:06+09:00",
                                40_000),
                        ContextPayload.Background.ok("world-index", "C", "{\"a\": 1}", "2026-10-20T16:30:07+09:00")
                                .withIndexRates(List.of(new ContextPayload.IndexRate("TESTIX", "TEST INDEX",
                                        "20261019", "100.00", null, null, null))),
                        ContextPayload.Background.missing("world-index", "S")),
                true);

        String json = serialize(payload);

        assertThat(ContractSchemas.validate("market.daily.responded.v1.schema.json", json)).isEmpty();
        assertThat(mapper.readTree(json).get("payload").get("background").get(0).has("target")).isTrue();
    }

    @Test
    void emptyResponseWithoutEtfsIsValid() throws IOException {
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(), List.of(),
                List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v1.schema.json", json)).isEmpty();
    }

    @Test
    void schemaRejectsDailyPriceBackgroundBecauseEtfDataGoesInEtfs() throws IOException {
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(), List.of(),
                List.of(ContextPayload.Background.missing("daily-price", "TEST01")), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v1.schema.json", json)).isNotEmpty();
    }

    @Test
    void schemaRejectsEtfWithoutItsRawRateOrWithAMalformedSymbol() throws IOException {
        String noRate = ContractSchemas.sample("market.daily.responded.sample.json")
                .replace("\"rate\": \"-1.50\",", "");
        String badSymbol = ContractSchemas.sample("market.daily.responded.sample.json")
                .replace("\"symbol\": \"TEST01\"", "\"symbol\": \"*\"");

        assertThat(ContractSchemas.validate("market.daily.responded.v1.schema.json", noRate)).isNotEmpty();
        assertThat(ContractSchemas.validate("market.daily.responded.v1.schema.json", badSymbol)).isNotEmpty();
    }

    private String serialize(DailyPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "market.daily.responded",
                OffsetDateTime.now(CLOCK), "daily-briefing-post-close-2026-10-20T17:00:00+09:00", payload));
    }
}

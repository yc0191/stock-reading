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
import dev.stockreading.market.event.EventFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class ContextContractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:43Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("market.context.requested.sample.json");

        assertThat(ContractSchemas.validate("market.context.requested.v1.schema.json", sample)).isEmpty();
        ContextRequestEnvelope event = mapper.readValue(sample, ContextRequestEnvelope.class);
        assertThat(event.eventType()).isEqualTo("market.context.requested");
        assertThat(event.payload().jobId()).isEqualTo(JOB_ID);
        assertThat(event.payload().seq()).isEqualTo(1);
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
        assertThat(event.payload().asOf()).isEqualTo("2026-10-20T16:00:31+09:00");
        assertThat(event.payload().since()).isEqualTo("2026-10-16T16:00:31+09:00");
        assertThat(event.payload().maxResponseBytes()).isEqualTo(32_768);
        assertThat(event.payload().maxTickers()).isEqualTo(100);
    }

    @Test
    void responseSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json",
                ContractSchemas.sample("market.context.responded.sample.json"))).isEmpty();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("market.context.requested.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("market.context.requested.v1.schema.json", withExtraFields)).isEmpty();
        assertThat(mapper.readValue(withExtraFields, ContextRequestEnvelope.class).payload().symbol())
                .isEqualTo("TEST01");
    }

    @Test
    void producerPayloadWithEveryBackgroundStatusMatchesSchema() throws IOException {
        ContextPayload payload = new ContextPayload(JOB_ID, 2, "TEST01", List.of("TESTA", "FAKEB"),
                List.of(new ContextPayload.CloseRate("TESTA", "20261019", "2026-10-20T05:00:00+09:00", "-1.25",
                        "2026-10-20T08:30:02+09:00")),
                List.of(
                        ContextPayload.Background.ok("exchange-rate-summary", null, "{\"a\": 1}",
                                "2026-10-20T15:55:01+09:00"),
                        ContextPayload.Background.tooLarge("market-summary", null, "2026-10-20T15:55:03+09:00",
                                40_000),
                        ContextPayload.Background.missing("world-index", "S"),
                        ContextPayload.Background.missing("daily-price", "TEST01")),
                true);

        String json = serialize(payload);

        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", json)).isEmpty();
        // target이 없는 종류도 null로 실려야 스키마의 필수 필드를 지킨다.
        assertThat(mapper.readTree(json).get("payload").get("background").get(0).has("target")).isTrue();
        assertThat(mapper.readTree(json).get("payload").get("background").get(2).has("fetchedAt")).isFalse();
    }

    @Test
    void responseWithoutUsTickersIsValid() throws IOException {
        String json = serialize(new ContextPayload(JOB_ID, 1, "TEST01", List.of(), List.of(),
                List.of(ContextPayload.Background.missing("daily-price", "TEST01")), false));

        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", json)).isEmpty();
    }

    @Test
    void schemaRejectsBackgroundWhoseFieldsDisagreeWithItsStatus() throws IOException {
        String okWithoutResponse = serialize(new ContextPayload(JOB_ID, 1, "TEST01", List.of(), List.of(),
                List.of(new ContextPayload.Background("market-summary", null, "ok", null,
                        "2026-10-20T15:55:03+09:00", null)), false));
        String missingWithResponse = serialize(new ContextPayload(JOB_ID, 1, "TEST01", List.of(), List.of(),
                List.of(new ContextPayload.Background("market-summary", null, "missing", "{}", null, null)), false));

        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", okWithoutResponse))
                .isNotEmpty();
        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", missingWithResponse))
                .isNotEmpty();
    }

    @Test
    void responseSampleCarriesTheComputedWorldIndexRatesNextToTheRawResponse() throws IOException {
        var world = mapper.readTree(ContractSchemas.sample("market.context.responded.sample.json"))
                .at("/payload/background/2");

        assertThat(world.get("apiType").asString()).isEqualTo("world-index");
        assertThat(world.has("response")).isTrue();
        assertThat(world.get("indexRates").get(0).get("rate").asString()).isEqualTo("2.04");
        assertThat(world.get("indexRates").get(1).get("rate").isNull()).isTrue();
    }

    @Test
    void producerPayloadWithIndexRatesMatchesSchemaEvenWhenARateIsMissing() throws IOException {
        List<ContextPayload.IndexRate> rates = List.of(
                new ContextPayload.IndexRate("TESTIX", "TEST INDEX", "20261020", "102.00", "20261019", "100.00", "2.00"),
                new ContextPayload.IndexRate("FAKEIX", "FAKE INDEX", "20261020", "50.00", null, null, null));
        String json = serialize(new ContextPayload(JOB_ID, 1, "TEST01", List.of(), List.of(), List.of(
                ContextPayload.Background.ok("world-index", "C", "{\"a\": 1}", "2026-10-20T15:55:05+09:00")
                        .withIndexRates(rates),
                ContextPayload.Background.tooLarge("world-index", "S", "2026-10-20T15:55:05+09:00", 40_000)
                        .withIndexRates(rates)), false));

        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", json)).isEmpty();
        // 없음은 키를 빼지 않고 null로 실어야 브리핑이 "없음"을 구분한다.
        assertThat(mapper.readTree(json).at("/payload/background/0/indexRates/1").has("prevClose")).isTrue();
    }

    @Test
    void schemaRejectsIndexRatesOutsideAWorldIndexThatHasData() throws IOException {
        List<ContextPayload.IndexRate> rates = List.of(
                new ContextPayload.IndexRate("TESTIX", "TEST INDEX", "20261020", "102.00", null, null, null));
        String onMarketSummary = serialize(new ContextPayload(JOB_ID, 1, "TEST01", List.of(), List.of(), List.of(
                ContextPayload.Background.ok("market-summary", null, "{}", "2026-10-20T15:55:03+09:00")
                        .withIndexRates(rates)), false));
        String onMissing = serialize(new ContextPayload(JOB_ID, 1, "TEST01", List.of(), List.of(), List.of(
                ContextPayload.Background.missing("world-index", "C").withIndexRates(rates)), false));
        String withoutRateKey = serialize(new ContextPayload(JOB_ID, 1, "TEST01", List.of(), List.of(), List.of(
                ContextPayload.Background.ok("world-index", "C", "{}", "2026-10-20T15:55:05+09:00")), false))
                .replace("\"status\":\"ok\"", "\"status\":\"ok\",\"indexRates\":[{\"code\":\"TESTIX\"}]");

        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", onMarketSummary)).isNotEmpty();
        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", onMissing)).isNotEmpty();
        assertThat(ContractSchemas.validate("market.context.responded.v1.schema.json", withoutRateKey)).isNotEmpty();
    }

    private String serialize(ContextPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "market.context.responded",
                OffsetDateTime.now(CLOCK), "collect-2026-10-20T16:00:30+09:00", payload));
    }
}

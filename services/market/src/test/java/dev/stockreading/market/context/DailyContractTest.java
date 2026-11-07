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
        assertThat(event.payload().wantsOperations()).isTrue();
    }

    @Test
    void requestWithoutTheOperationsFlagStillMatchesTheSchemaAndDoesNotAskForOperations() throws IOException {
        String withoutFlag = ContractSchemas.sample("market.daily.requested.sample.json")
                .replace(",\n    \"includeOperations\": true", "");

        assertThat(withoutFlag).doesNotContain("includeOperations");
        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", withoutFlag)).isEmpty();
        assertThat(mapper.readValue(withoutFlag, DailyRequestEnvelope.class).payload().wantsOperations()).isFalse();
    }

    @Test
    void schemaRejectsANonBooleanOperationsFlag() throws IOException {
        String bad = ContractSchemas.sample("market.daily.requested.sample.json")
                .replace("\"includeOperations\": true", "\"includeOperations\": \"yes\"");

        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", bad)).isNotEmpty();
    }

    @Test
    void v1ResponseSampleStillMatchesTheV1Schema() throws IOException {
        assertThat(ContractSchemas.validate("market.daily.responded.v1.schema.json",
                ContractSchemas.sample("market.daily.responded.sample.json"))).isEmpty();
    }

    @Test
    void v2ResponseSampleMatchesTheV2SchemaAndIncludesAnEtfWithoutADailyPrice() throws IOException {
        String sample = ContractSchemas.sample("v2", "market.daily.responded.sample.json");

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", sample)).isEmpty();
        var etfs = mapper.readTree(sample).get("payload").get("etfs");
        assertThat(mapper.readTree(sample).get("schemaVersion").asInt()).isEqualTo(2);
        assertThat(etfs.get(0).get("missingReason").isNull()).isTrue();
        assertThat(etfs.get(2).get("close").isNull()).isTrue();
        assertThat(etfs.get(2).get("missingReason").asString()).isEqualTo("no-daily-price-in-range");
        assertThat(etfs.get(2).get("tickers")).hasSize(1);
    }

    @Test
    void v2SampleOperationsMatchTheSchemaWithAllFourCollectionsAndEveryFailureKind() throws IOException {
        String sample = ContractSchemas.sample("v2", "market.daily.responded.sample.json");

        var operations = mapper.readTree(sample).get("payload").get("operations");
        assertThat(operations.get("collections")).hasSize(4);
        assertThat(operations.get("collections").get(3).get("firstReceivedAt").isNull()).isTrue();
        assertThat(operations.get("quoteFailures").has("TICK_SKIPPED_SUPERSEDED")).isTrue();
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json",
                sample.replace("\"TIMEOUT\": 0,", ""))).isNotEmpty();
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json",
                sample.replace("\"receivedCount\": 4", "\"receivedCount\": -1"))).isNotEmpty();
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json",
                sample.replace("\"kind\": \"pre-open\"", "\"kind\": \"night\""))).isNotEmpty();
    }

    @Test
    void producerOperationsMatchSchemaAndAreLeftOutWhenNull() throws IOException {
        var failures = new java.util.LinkedHashMap<String, Integer>();
        OperationsReader.QUOTE_FAILURE_KINDS.forEach(kind -> failures.put(kind, 0));
        failures.put("SOMETHING_NEW", 2);
        DailyPayload.Operations operations = new DailyPayload.Operations(
                OperationsReader.COLLECTION_KINDS.stream()
                        .map(kind -> new DailyPayload.CollectionStatus(kind, null, null, 0)).toList(),
                failures, List.of(new DailyPayload.DroppedTopic("market.snapshot", 3)));
        DailyPayload base = new DailyPayload(JOB_ID, 1, List.of(), List.of(),
                List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false);

        String with = serialize(new DailyPayload(JOB_ID, 1, List.of(), List.of(),
                List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false, operations));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", with)).isEmpty();
        assertThat(mapper.readTree(with).get("payload").get("operations").get("quoteFailures").get("SOMETHING_NEW")
                .asInt()).isEqualTo(2);
        assertThat(mapper.readTree(serialize(base)).get("payload").has("operations")).isFalse();
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

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
        assertThat(mapper.readTree(json).get("payload").get("background").get(0).has("target")).isTrue();
    }

    @Test
    void producerEtfInfoMatchesSchemaAndMissingCellsAreLeftOut() throws IOException {
        ContextPayload.EtfInfo info = new ContextPayload.EtfInfo("TEST ETF", null, null, List.of(
                new ContextPayload.EtfMember("US", "TESTA", null, "12.5", 1)), false);
        String json = serialize(new DailyPayload(JOB_ID, 1,
                List.of(new DailyPayload.Etf("TEST01", "20261020", "9850", "10000", "-1.50",
                        "2026-10-20T16:30:02+09:00", List.of("TESTA"), false, info),
                        new DailyPayload.Etf("TEST02", "20261020", "20100", "20000", "0.50",
                                "2026-10-20T16:30:03+09:00", List.of(), false)),
                List.of(), List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
        var etfs = mapper.readTree(json).get("payload").get("etfs");
        assertThat(etfs.get(0).get("etf").has("tracks")).isFalse();
        assertThat(etfs.get(0).get("etf").get("members").get(0).has("name")).isFalse();
        assertThat(etfs.get(1).has("etf")).isFalse();
    }

    @Test
    void emptyResponseWithoutEtfsIsValid() throws IOException {
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(), List.of(),
                List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
    }

    @Test
    void schemaRejectsDailyPriceBackgroundBecauseEtfDataGoesInEtfs() throws IOException {
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(), List.of(),
                List.of(ContextPayload.Background.missing("daily-price", "TEST01")), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isNotEmpty();
    }

    @Test
    void schemaRejectsEtfWithoutItsRawRateOrWithAMalformedSymbol() throws IOException {
        String noRate = ContractSchemas.sample("v2", "market.daily.responded.sample.json")
                .replace("\"rate\": \"-1.50\",", "");
        String badSymbol = ContractSchemas.sample("v2", "market.daily.responded.sample.json")
                .replace("\"symbol\": \"TEST01\"", "\"symbol\": \"*\"");

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", noRate)).isNotEmpty();
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", badSymbol)).isNotEmpty();
    }

    @Test
    void producerEtfWithoutADailyPriceCarriesNullsAndTheReasonAndStillMatchesSchema() throws IOException {
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(
                DailyPayload.Etf.withoutDaily("TEST01", DailyPayload.NO_DAILY_PRICE_IN_RANGE, List.of("TESTA"), false,
                        null),
                DailyPayload.Etf.withoutDaily("TEST02", DailyPayload.DAILY_PRICE_UNREADABLE, List.of(), false, null)),
                List.of(), List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
        var etfs = mapper.readTree(json).get("payload").get("etfs");
        assertThat(etfs.get(0).get("date").isNull()).isTrue();
        assertThat(etfs.get(0).get("fetchedAt").isNull()).isTrue();
        assertThat(etfs.get(0).get("missingReason").asString()).isEqualTo("no-daily-price-in-range");
        assertThat(etfs.get(1).get("missingReason").asString()).isEqualTo("daily-price-unreadable");
        assertThat(mapper.readTree(json).get("schemaVersion").asInt()).isEqualTo(2);
    }

    @Test
    void schemaRejectsInconsistentDailyCellsAndUnknownReasons() throws IOException {
        String sample = ContractSchemas.sample("v2", "market.daily.responded.sample.json");
        // 사유가 있는데 값이 남아 있음, 사유가 없는데 값이 비어 있음, 정해진 목록 밖의 사유.
        String reasonWithValues = sample.replaceFirst("\"missingReason\": null",
                "\"missingReason\": \"no-daily-price-in-range\"");
        String nullsWithoutReason = sample.replace("\"close\": \"9850\"", "\"close\": null");
        String unknownReason = sample.replace("\"no-daily-price-in-range\"", "\"because\"");
        String noReasonKey = sample.replaceFirst("\"missingReason\": null,", "");

        for (String bad : List.of(reasonWithValues, nullsWithoutReason, unknownReason, noReasonKey)) {
            assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", bad)).isNotEmpty();
        }
    }

    private String serialize(DailyPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "market.daily.responded", 2,
                OffsetDateTime.now(CLOCK), "daily-briefing-post-close-2026-10-20T17:00:00+09:00", payload));
    }
}

package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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

/**
 * 내부 조회 API 의 요청·응답 본문은 옛 요청·응답 이벤트의 payload 계약(contracts/events/, 더 이상 쓰지 않는 토픽)과 같다.
 * 남겨 둔 샘플을 지금 요청 본문으로 계속 읽을 수 있고, 지금 응답 본문이 그 계약을 지키는지 본다(ADR-010).
 */
class DailyContractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T08:00:01Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "8d3b5e17-6c2a-4f90-b1e4-7a9c0d2f3e68";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("market.daily.requested.sample.json");

        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", sample)).isEmpty();
        DailyRequest request = payload(sample, DailyRequest.class);
        assertThat(request.jobId()).isEqualTo(JOB_ID);
        assertThat(request.seq()).isEqualTo(1);
        assertThat(request.asOf()).isEqualTo("2026-10-20T17:00:00+09:00");
        assertThat(request.since()).isEqualTo("2026-10-16T17:00:00+09:00");
        assertThat(request.maxResponseBytes()).isEqualTo(32_768);
        assertThat(request.maxEtfs()).isEqualTo(50);
        assertThat(request.maxTickers()).isEqualTo(100);
        assertThat(request.wantsOperations()).isTrue();
        assertThat(request.wantsIntradayFallback()).isTrue();
    }

    @Test
    void requestWithoutTheIntradayFallbackFlagStillMatchesTheSchemaAndDoesNotAskForIt() throws IOException {
        String sample = ContractSchemas.sample("market.daily.requested.sample.json");
        String withoutFlag = sample.replace(",\n    \"includeIntradayFallback\": true", "");

        assertThat(withoutFlag).doesNotContain("includeIntradayFallback");
        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", withoutFlag)).isEmpty();
        assertThat(payload(withoutFlag, DailyRequest.class).wantsIntradayFallback()).isFalse();
        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json",
                sample.replace("\"includeIntradayFallback\": true", "\"includeIntradayFallback\": \"yes\""))).isNotEmpty();
    }

    @Test
    void requestWithoutTheOperationsFlagStillMatchesTheSchemaAndDoesNotAskForOperations() throws IOException {
        String withoutFlag = ContractSchemas.sample("market.daily.requested.sample.json")
                .replace(",\n    \"includeOperations\": true", "");

        assertThat(withoutFlag).doesNotContain("includeOperations");
        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", withoutFlag)).isEmpty();
        assertThat(payload(withoutFlag, DailyRequest.class).wantsOperations()).isFalse();
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
        assertThat(payload(withExtraFields, DailyRequest.class).maxEtfs()).isEqualTo(50);
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
    void producerMemberChangesMatchSchemaAndAreLeftOutWhenThereIsNoChange() throws IOException {
        ContextPayload.EtfInfo changed = new ContextPayload.EtfInfo("TEST ETF", null, null, List.of(), false)
                .withMemberChanges(new ContextPayload.MemberChanges("2026-10-12", "2026-10-19",
                        List.of(new ContextPayload.ChangedMember("US", "OLDC", null, 3)),
                        List.of(new ContextPayload.ChangedMember("US", "NEWD", "NEW CORP D", 2)),
                        List.of(new ContextPayload.MovedMember("KR", "TEST09", null, 2, 1))));
        ContextPayload.EtfInfo unchanged = new ContextPayload.EtfInfo("TEST ETF TWO", null, null, List.of(), false);
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(
                new DailyPayload.Etf("TEST01", "20261020", "9850", "10000", "-1.50", "2026-10-20T16:30:02+09:00",
                        List.of(), false, changed),
                new DailyPayload.Etf("TEST02", "20261020", "20100", "20000", "0.50", "2026-10-20T16:30:03+09:00",
                        List.of(), false, unchanged)),
                List.of(), List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
        var etfs = mapper.readTree(json).get("payload").get("etfs");
        var memberChanges = etfs.get(0).get("etf").get("memberChanges");
        assertThat(memberChanges.get("since").asString()).isEqualTo("2026-10-12");
        assertThat(memberChanges.get("removed").get(0).has("name")).isFalse();
        assertThat(memberChanges.get("moved").get(0).get("rankAfter").asInt()).isEqualTo(1);
        assertThat(etfs.get(1).get("etf").has("memberChanges")).isFalse();
    }

    @Test
    void sampleMemberChangesMatchTheSchemaAndABrokenOneIsRejected() throws IOException {
        String sample = ContractSchemas.sample("v2", "market.daily.responded.sample.json");
        assertThat(sample).contains("\"memberChanges\"");
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", sample)).isEmpty();

        // 순위 0, 날짜 모양 틀림, 필수 칸(moved) 없음은 계약 위반이다.
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json",
                sample.replace("\"rankBefore\": 2", "\"rankBefore\": 0"))).isNotEmpty();
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json",
                sample.replace("\"2026-10-12\"", "\"10/12\""))).isNotEmpty();
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json",
                sample.replaceFirst("\"moved\": \\[", "\"moved_\": ["))).isNotEmpty();
    }

    @Test
    void etfCarriesNoHeldOrWatchedBecauseThatBelongsToEachMembersPortfolio() throws IOException {
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(
                DailyPayload.Etf.withoutDaily("TEST01", DailyPayload.NO_DAILY_PRICE_IN_RANGE, List.of(), false, null),
                DailyPayload.Etf.withoutDaily("TEST02", DailyPayload.NO_DAILY_PRICE_IN_RANGE, List.of(), false, null)),
                List.of(), List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
        assertThat(json).doesNotContain("\"relation\"");
        String sample = ContractSchemas.sample("v2", "market.daily.responded.sample.json");
        assertThat(sample).doesNotContain("\"relation\"");
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", sample)).isEmpty();
        // 스키마가 추가 칸을 막지 않으므로 칸 이름이 스키마에서 빠졌는지는 정의 쪽에서 확인한다.
        assertThat(Files.readString(Path.of("../../contracts/events/market.daily.responded.v2.schema.json")))
                .doesNotContain("\"relation\"");
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

    @Test
    void producerIntradayFallbackMatchesTheSchemaAndIsLeftOutForADailyPrice() throws IOException {
        DailyPayload.Etf plain = new DailyPayload.Etf("TEST01", "20261020", "9850", "10000", "-1.50",
                "2026-10-20T16:30:02+09:00", List.of(), false);
        DailyPayload.Etf fallback = DailyPayload.Etf.withoutDaily("TEST04", DailyPayload.NO_DAILY_PRICE_IN_RANGE,
                List.of(), false, null).withIntradayFallback("10000", "-2.95",
                new DailyPayload.IntradayFallback("9705", "2026-10-20T15:29:58+09:00"));
        String json = serialize(new DailyPayload(JOB_ID, 1, List.of(plain, fallback), List.of(),
                List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
        var etfs = mapper.readTree(json).get("payload").get("etfs");
        assertThat(etfs.get(0).has("intradayFallback")).isFalse();
        assertThat(etfs.get(1).get("rate").asString()).isEqualTo("-2.95");
        assertThat(etfs.get(1).get("missingReason").asString()).isEqualTo("no-daily-price-in-range");
        assertThat(etfs.get(1).get("intradayFallback").get("receivedAt").asString())
                .isEqualTo("2026-10-20T15:29:58+09:00");
    }

    @Test
    void schemaRejectsAFallbackWithDailyCellsOrWithoutItsRate() throws IOException {
        String sample = ContractSchemas.sample("v2", "market.daily.responded.sample.json");
        assertThat(sample).contains("\"intradayFallback\"");
        // 대체값인데 일봉 값(일자)이 있음, 대체값인데 등락률이 비어 있음, 받은 시각이 시각 형식이 아님,
        // 일봉이 있는 ETF에 대체값 칸이 붙음, 대체값 없이 사유와 등락률이 함께 있음.
        String withDate = sample.replace("\"rate\": \"-2.95\",\n        \"fetchedAt\": null",
                "\"rate\": \"-2.95\",\n        \"fetchedAt\": \"2026-10-20T16:30:02+09:00\"");
        String withoutRate = sample.replace("\"rate\": \"-2.95\"", "\"rate\": null");
        String badTime = sample.replace("\"receivedAt\": \"2026-10-20T15:29:58+09:00\"", "\"receivedAt\": \"15:29\"");
        String onDaily = sample.replaceFirst("\"missingReason\": null,",
                "\"missingReason\": null, \"intradayFallback\": {\"price\": \"9850\", \"receivedAt\": \"2026-10-20T15:29:58+09:00\"},");
        String noFallback = sample.replaceFirst("\"intradayFallback\": \\{[^}]*\\},", "");

        for (String bad : List.of(withDate, withoutRate, badTime, onDaily, noFallback)) {
            assertThat(bad).isNotEqualTo(sample);
            assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", bad)).isNotEmpty();
        }
    }

    private String serialize(DailyPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "market.daily.responded", 2,
                OffsetDateTime.now(CLOCK), "daily-briefing-post-close-2026-10-20T17:00:00+09:00", payload));
    }

    @Test
    void requestWithOpenFlowSinceMatchesTheSchemaAndRejectsANonTimestamp() throws IOException {
        String sample = ContractSchemas.sample("market.daily.requested.sample.json");
        String with = sample.replace("\"includeOperations\": true",
                "\"includeOperations\": true,\n    \"openFlowSince\": \"2026-10-20T16:50:00+09:00\"");

        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json", with)).isEmpty();
        assertThat(payload(with, DailyRequest.class).openFlowSince())
                .isEqualTo("2026-10-20T16:50:00+09:00");
        assertThat(payload(sample, DailyRequest.class).openFlowSince()).isNull();
        assertThat(ContractSchemas.validate("market.daily.requested.v1.schema.json",
                with.replace("2026-10-20T16:50:00+09:00", "ten to five"))).isNotEmpty();
    }

    @Test
    void producerOpenFlowMatchesSchemaAndIsLeftOutWhenNull() throws IOException {
        DailyPayload.OpenFlow present = new DailyPayload.OpenFlow("present", "10000", "10200", "raw", "2.00",
                "2026-10-20T09:00:01+09:00", "10100", "2026-10-20T09:09:58+09:00", "10300", "1.98", "3.00");
        DailyPayload.OpenFlow partial = new DailyPayload.OpenFlow("present", null, "10100", "first-snapshot", null,
                "2026-10-20T09:00:01+09:00", "10100", "2026-10-20T09:09:58+09:00", "10300", "1.98", null);
        DailyPayload.Etf plain = new DailyPayload.Etf("TEST01", "20261020", "9850", "10000", "-1.50",
                "2026-10-20T16:30:02+09:00", List.of(), false);
        String json = serialize(new DailyPayload(JOB_ID, 1,
                List.of(plain.withOpenFlow(present), plain.withOpenFlow(partial),
                        plain.withOpenFlow(DailyPayload.OpenFlow.none()), plain),
                List.of(), List.of(ContextPayload.Background.missing("exchange-rate-summary", null)), false));

        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json", json)).isEmpty();
        var etfs = mapper.readTree(json).get("payload").get("etfs");
        assertThat(etfs.get(0).get("openFlow").get("openRate").asString()).isEqualTo("2.00");
        assertThat(etfs.get(1).get("openFlow").get("prevClose").isNull()).isTrue();
        assertThat(etfs.get(2).get("openFlow").get("status").asString()).isEqualTo("no-snapshot");
        assertThat(etfs.get(3).has("openFlow")).isFalse();
        assertThat(ContractSchemas.validate("market.daily.responded.v2.schema.json",
                json.replace("\"status\":\"no-snapshot\"", "\"status\":\"other\""))).isNotEmpty();
    }

    /** 요청 이벤트 샘플의 payload 를 내부 조회 API 요청 본문으로 읽는다. 요청 본문은 그 payload 와 같은 모양이다. */
    private <T> T payload(String event, Class<T> type) {
        return mapper.treeToValue(mapper.readTree(event).get("payload"), type);
    }
}

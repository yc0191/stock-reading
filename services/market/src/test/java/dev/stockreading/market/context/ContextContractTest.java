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

    private String serialize(ContextPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "market.context.responded",
                OffsetDateTime.now(CLOCK), "collect-2026-10-20T16:00:30+09:00", payload));
    }
}

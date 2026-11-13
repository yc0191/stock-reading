package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class SnapshotContractTest {

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void v1SampleMatchesSchema() throws IOException {
        var errors = ContractSchemas.validate("market.snapshot.v1.schema.json", ContractSchemas.sample("market.snapshot.sample.json"));
        assertThat(errors).isEmpty();
    }

    @Test
    void v1SampleIsReadableByCurrentConsumer() throws IOException {
        SnapshotEnvelope event = mapper.readValue(ContractSchemas.sample("market.snapshot.sample.json"), SnapshotEnvelope.class);

        assertThat(event.eventType()).isEqualTo("market.snapshot");
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
        assertThat(event.payload().price()).isEqualTo(10_250L);
        assertThat(event.payload().raw()).isNotBlank();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("market.snapshot.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", withExtraFields)).isEmpty();
        SnapshotEnvelope event = mapper.readValue(withExtraFields, SnapshotEnvelope.class);
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
    }

    @Test
    void snapshotWithoutRawFromOlderProducersIsStillReadable() throws IOException {
        String withoutRaw = ContractSchemas.sample("market.snapshot.sample.json")
                .replaceAll(",\\s*\"raw\": \"(\\\\.|[^\"\\\\])*\"", "");

        assertThat(withoutRaw).doesNotContain("raw");
        assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", withoutRaw)).isEmpty();
        assertThat(mapper.readValue(withoutRaw, SnapshotEnvelope.class).payload().raw()).isNull();
    }

    @Test
    void failureSampleMatchesSchemaAndIsReadableByConsumer() throws IOException {
        String sample = ContractSchemas.sample("market.snapshot.failed.sample.json");

        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", sample)).isEmpty();
        FailureEnvelope event = mapper.readValue(sample, FailureEnvelope.class);
        assertThat(event.eventType()).isEqualTo("market.snapshot.failed");
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
        assertThat(event.payload().failureType()).isEqualTo("HTTP_ERROR");
        assertThat(event.payload().httpStatus()).isEqualTo(500);
        assertThat(event.payload().tokenState().remainingMs()).isEqualTo(82_800_000L);
        assertThat(event.payload().responseRaw()).isNotBlank();
    }

    @Test
    void fieldMismatchFailureMatchesSchemaAndIsReadableByConsumer() throws IOException {
        String mismatch = ContractSchemas.sample("market.snapshot.failed.sample.json")
                .replace("\"HTTP_ERROR\"", "\"FIELD_MISMATCH\"")
                .replace("\"HttpStatusException\"", "\"FieldMismatch:CHANGE+RATE+HIGH_LOW\"");

        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", mismatch)).isEmpty();
        FailureEnvelope event = mapper.readValue(mismatch, FailureEnvelope.class);
        assertThat(event.payload().failureType()).isEqualTo("FIELD_MISMATCH");
        assertThat(event.payload().exceptionType()).isEqualTo("FieldMismatch:CHANGE+RATE+HIGH_LOW");
        assertThat(event.payload().responseRaw()).isNotBlank();
    }

    @Test
    void failureConsumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("market.snapshot.failed.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", withExtraFields)).isEmpty();
        assertThat(mapper.readValue(withExtraFields, FailureEnvelope.class).payload().symbol()).isEqualTo("TEST01");
    }

    @Test
    void failureCarriesNoCredentialFields() throws IOException {
        String schema = java.nio.file.Files.readString(java.nio.file.Path.of("../../contracts/events/market.snapshot.failed.v1.schema.json"));

        assertThat(schema.toLowerCase()).doesNotContain("authorization").doesNotContain("appkey").doesNotContain("secret");
    }

    @Test
    void moveSampleWithDetectionFieldsMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate("market.move.v1.schema.json", ContractSchemas.sample("market.move.sample.json"))).isEmpty();
        assertThat(ContractSchemas.sample("market.move.sample.json"))
                .contains("\"detectionType\": \"window\"").contains("moveStartedAt").contains("todayChangePercent");
    }

    @Test
    void moveWithoutTodayChangeStillMatchesSchema() throws IOException {
        String older = ContractSchemas.sample("market.move.sample.json").replaceAll(",\\s*\"todayChangePercent\": [-0-9.]+", "");

        assertThat(older).doesNotContain("todayChangePercent");
        assertThat(ContractSchemas.validate("market.move.v1.schema.json", older)).isEmpty();
    }

    @Test
    void moveWithUnknownDetectionTypeIsRejectedBySchema() throws IOException {
        String broken = ContractSchemas.sample("market.move.sample.json").replace("\"window\"", "\"other\"");

        assertThat(ContractSchemas.validate("market.move.v1.schema.json", broken)).isNotEmpty();
    }

    @Test
    void producedMoveMatchesSchemaWithAndWithoutDetectionFields() throws IOException {
        var withFields = new dev.stockreading.market.event.MovePayload(
                "TEST01", 10_000, 9_850, new java.math.BigDecimal("-1.50"), new java.math.BigDecimal("1.0"),
                "2026-10-20T16:00:31+09:00", "prevClose", "2026-10-20T16:00:31+09:00", new java.math.BigDecimal("-1.50"));
        var legacy = new dev.stockreading.market.event.MovePayload(
                "TEST01", 10_000, 9_850, new java.math.BigDecimal("-1.50"), new java.math.BigDecimal("1.0"),
                "2026-10-20T16:00:31+09:00");
        java.time.Clock clock = java.time.Clock.systemUTC();
        java.time.OffsetDateTime at = java.time.OffsetDateTime.parse("2026-10-20T16:00:31+09:00");

        for (var payload : java.util.List.of(withFields, legacy)) {
            String json = mapper.writeValueAsString(dev.stockreading.market.event.EventFactory.create(
                    clock, "market.move", at, "collect-x", payload));
            assertThat(ContractSchemas.validate("market.move.v1.schema.json", json)).isEmpty();
        }
        assertThat(mapper.writeValueAsString(legacy)).doesNotContain("detectionType").doesNotContain("todayChangePercent")
                .doesNotContain("null");
    }
}

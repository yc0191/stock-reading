package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import dev.stockreading.collector.marketdata.FailureType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class EventContractTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    // 정각 16:00:00: OffsetDateTime.toString()이 초를 생략하는 경우.
    private static final Clock SHARP_CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), KST);

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void committedSampleTickMatchesSchema() throws IOException {
        var errors = ContractSchemas.validate("schedule.tick.v1.schema.json", ContractSchemas.sample("schedule.tick.sample.json"));
        assertThat(errors).isEmpty();
    }

    @Test
    void committedSampleSnapshotMatchesSchema() throws IOException {
        var errors = ContractSchemas.validate("market.snapshot.v1.schema.json", ContractSchemas.sample("market.snapshot.sample.json"));
        assertThat(errors).isEmpty();
    }

    @Test
    void producedTickMatchesSchemaEvenWhenSecondsAreZero() throws IOException {
        OffsetDateTime scheduledFor = OffsetDateTime.now(SHARP_CLOCK);
        String text = EventFactory.format(scheduledFor);
        Envelope<TickPayload> tick = EventFactory.create(
                SHARP_CLOCK, "schedule.tick", scheduledFor, "collect-" + text, new TickPayload("collect", text));

        var errors = ContractSchemas.validate("schedule.tick.v1.schema.json", mapper.writeValueAsString(tick));

        assertThat(errors).isEmpty();
        assertThat(text).isEqualTo("2026-10-20T16:00:00+09:00");
    }

    @Test
    void producedSnapshotMatchesSchema() throws IOException {
        OffsetDateTime asOf = OffsetDateTime.now(SHARP_CLOCK);
        Envelope<SnapshotPayload> snapshot = EventFactory.create(
                SHARP_CLOCK, "market.snapshot", asOf, "collect-x",
                new SnapshotPayload("TEST01", 10_250, EventFactory.format(asOf), "simulator"));

        var errors = ContractSchemas.validate("market.snapshot.v1.schema.json", mapper.writeValueAsString(snapshot));

        assertThat(errors).isEmpty();
    }

    @Test
    void producedSnapshotWithRawMatchesSchemaAndOmitsRawWhenAbsent() throws IOException {
        OffsetDateTime asOf = OffsetDateTime.now(SHARP_CLOCK);
        String time = EventFactory.format(asOf);
        Envelope<SnapshotPayload> withRaw = EventFactory.create(
                SHARP_CLOCK, "market.snapshot", asOf, "collect-x",
                new SnapshotPayload("TEST01", 10_250, time, "simulator", "{\"example\":\"fake\"}"));
        Envelope<SnapshotPayload> withoutRaw = EventFactory.create(
                SHARP_CLOCK, "market.snapshot", asOf, "collect-x",
                new SnapshotPayload("TEST01", 10_250, time, "simulator"));

        assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", mapper.writeValueAsString(withRaw))).isEmpty();
        String omitted = mapper.writeValueAsString(withoutRaw);
        assertThat(omitted).doesNotContain("raw");
        assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", omitted)).isEmpty();
    }

    @Test
    void committedSampleFailureMatchesSchema() throws IOException {
        var errors = ContractSchemas.validate(
                "market.snapshot.failed.v1.schema.json", ContractSchemas.sample("market.snapshot.failed.sample.json"));
        assertThat(errors).isEmpty();
    }

    @Test
    void producedFailureMatchesSchemaWithOnlyRequiredFieldsAndWithAllFields() throws IOException {
        OffsetDateTime at = OffsetDateTime.now(SHARP_CLOCK);
        String time = EventFactory.format(at);
        Envelope<FailurePayload> minimal = EventFactory.create(
                SHARP_CLOCK, "market.snapshot.failed", at, "collect-" + time,
                new FailurePayload("TEST01", "TICK_SKIPPED", time, null, null, null, null, null, null, null, null, null));
        Envelope<FailurePayload> full = EventFactory.create(
                SHARP_CLOCK, "market.snapshot.failed", at, "collect-" + time,
                new FailurePayload("TEST01", "HTTP_ERROR", time, 500, "E1001", 1450L, "HttpStatusException",
                        new FailurePayload.TokenState(3_600_000L, 82_800_000L), 3, 2, 1, "{\"example\":\"fake\"}"));

        String minimalJson = mapper.writeValueAsString(minimal);
        assertThat(minimalJson).doesNotContain("httpStatus").doesNotContain("null");
        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", minimalJson)).isEmpty();
        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", mapper.writeValueAsString(full))).isEmpty();
    }

    @Test
    void producedFieldMismatchFailureWithRawResponseMatchesSchema() throws IOException {
        OffsetDateTime at = OffsetDateTime.now(SHARP_CLOCK);
        String time = EventFactory.format(at);
        Envelope<FailurePayload> mismatch = EventFactory.create(
                SHARP_CLOCK, "market.snapshot.failed", at, "collect-" + time,
                new FailurePayload("TEST01", FailureType.FIELD_MISMATCH.name(), time, null, null, 12L,
                        "FieldMismatch:CHANGE+RATE", null, 2, 1, 1, "{\"dataBody\":{\"price\":\"99999\"}}"));

        String json = mapper.writeValueAsString(mismatch);
        assertThat(json).contains("\"failureType\":\"FIELD_MISMATCH\"").contains("responseRaw");
        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", json)).isEmpty();
    }

    @Test
    void everyFailureTypeIsAllowedBySchema() throws IOException {
        for (FailureType type : FailureType.values()) {
            String sample = ContractSchemas.sample("market.snapshot.failed.sample.json")
                    .replace("\"HTTP_ERROR\"", "\"" + type.name() + "\"");
            assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", sample)).as(type.name()).isEmpty();
        }
    }

    @Test
    void failureWithUnknownFailureTypeIsRejectedBySchema() throws IOException {
        String broken = ContractSchemas.sample("market.snapshot.failed.sample.json")
                .replace("\"HTTP_ERROR\"", "\"SOMETHING_ELSE\"");

        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", broken)).isNotEmpty();
    }

    @Test
    void timestampWithoutSecondsIsRejectedProvingFormatAssertionsAreOn() throws IOException {
        String withoutSeconds = ContractSchemas.sample("schedule.tick.sample.json")
                .replace("\"occurredAt\": \"2026-10-20T16:00:00+09:00\"", "\"occurredAt\": \"2026-10-20T16:00+09:00\"");

        var errors = ContractSchemas.validate("schedule.tick.v1.schema.json", withoutSeconds);

        assertThat(errors).isNotEmpty();
    }

    @Test
    void snapshotWithInvalidSymbolIsRejectedBySchema() throws IOException {
        OffsetDateTime asOf = OffsetDateTime.now(SHARP_CLOCK);
        Envelope<SnapshotPayload> bad = EventFactory.create(
                SHARP_CLOCK, "market.snapshot", asOf, "collect-x",
                new SnapshotPayload("bad", 10_250, EventFactory.format(asOf), "simulator"));

        var errors = ContractSchemas.validate("market.snapshot.v1.schema.json", mapper.writeValueAsString(bad));

        assertThat(errors).isNotEmpty();
    }
}

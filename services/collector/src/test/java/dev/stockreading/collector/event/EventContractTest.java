package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

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

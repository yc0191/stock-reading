package dev.stockreading.market.holdings;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.Envelope;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.event.HoldingsPayload;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 계약 샘플과 실제로 만드는 이벤트가 스키마를 지키는지 확인한다(ADR-003/010). */
class HoldingsContractTest {

    private static final String SCHEMA = "market.holdings.v1.schema.json";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void sampleMatchesTheSchemaWithAndWithoutAName() throws IOException {
        String sample = ContractSchemas.sample("market.holdings.sample.json");

        assertThat(ContractSchemas.validate(SCHEMA, sample)).isEmpty();
        JsonNode instruments = mapper.readTree(sample).get("payload").get("instruments");
        assertThat(instruments.get(0).has("name")).isTrue();
        assertThat(instruments.get(1).has("name")).isFalse();
    }

    @Test
    void theEventTheProducerBuildsMatchesTheSchemaAndOmitsAMissingName() throws IOException {
        HoldingsSnapshot snapshot = new HoldingsSnapshot(
                List.of(new HoldingsPayload.Instrument("ETF", "KR", "TEST01", "HELD", "테스트 ETF"),
                        new HoldingsPayload.Instrument("STOCK", "US", "FAKE-B", "NONE", null)),
                List.of(new HoldingsPayload.Membership("KR", "TEST01", "US", "FAKE-B", null)));
        Clock clock = Clock.fixed(Instant.parse("2026-11-10T01:00:30Z"), ZoneId.of("Asia/Seoul"));
        Envelope<HoldingsPayload> event = EventFactory.create(clock, Topics.MARKET_HOLDINGS,
                OffsetDateTime.now(clock), "holdings-change-test", snapshot.toPayload());

        String json = mapper.writeValueAsString(event);

        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        JsonNode payload = mapper.readTree(json).get("payload");
        assertThat(payload.get("instruments").get(1).has("name")).isFalse();
        assertThat(payload.get("memberships").get(0).get("rank").isNull()).isTrue();
        assertThat(mapper.readTree(json).get("eventType").asString()).isEqualTo("market.holdings");
    }

    @Test
    void emptyListMatchesTheSchema() throws IOException {
        Clock clock = Clock.fixed(Instant.parse("2026-11-10T01:00:30Z"), ZoneId.of("Asia/Seoul"));
        String json = mapper.writeValueAsString(EventFactory.create(clock, Topics.MARKET_HOLDINGS,
                OffsetDateTime.now(clock), "holdings-change-test",
                new HoldingsSnapshot(List.of(), List.of()).toPayload()));

        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
    }

    @Test
    void schemaRejectsABadHashAndABadRelationAndAMissingList() throws IOException {
        String sample = ContractSchemas.sample("market.holdings.sample.json");

        assertThat(ContractSchemas.validate(SCHEMA, sample.replaceFirst("\"contentHash\": \"[0-9a-f]+\"",
                "\"contentHash\": \"XYZ\""))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("\"WATCH\"", "\"OWNED\""))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA, sample.replace("\"memberships\"", "\"members\""))).isNotEmpty();
    }

    @Test
    void consumersIgnoreFieldsAddedInFutureVersions() throws IOException {
        String future = ContractSchemas.sample("market.holdings.sample.json")
                .replace("\"contentHash\"", "\"newField\": 1, \"contentHash\"");

        assertThat(ContractSchemas.validate(SCHEMA, future)).isEmpty();
    }
}

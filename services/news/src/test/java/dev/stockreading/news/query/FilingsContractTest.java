package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.news.event.EventFactory;
import dev.stockreading.news.event.FilingsPayload;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class FilingsContractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("news.filings.requested.sample.json");

        assertThat(ContractSchemas.validate("news.filings.requested.v1.schema.json", sample)).isEmpty();
        FilingsRequestEnvelope event = mapper.readValue(sample, FilingsRequestEnvelope.class);
        assertThat(event.eventType()).isEqualTo("news.filings.requested");
        assertThat(event.payload().jobId()).isEqualTo(JOB_ID);
        assertThat(event.payload().seq()).isEqualTo(1);
        assertThat(event.payload().symbol()).isEqualTo("TEST01");
        assertThat(event.payload().tickers()).containsExactly("TESTA", "FAKEB");
        assertThat(event.payload().since()).isEqualTo("2026-10-17T16:00:31+09:00");
        assertThat(event.payload().until()).isEqualTo("2026-10-20T16:00:31+09:00");
        assertThat(event.payload().maxFilings()).isEqualTo(20);
    }

    @Test
    void responseSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate("news.filings.responded.v1.schema.json",
                ContractSchemas.sample("news.filings.responded.sample.json"))).isEmpty();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("news.filings.requested.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("news.filings.requested.v1.schema.json", withExtraFields)).isEmpty();
        assertThat(mapper.readValue(withExtraFields, FilingsRequestEnvelope.class).payload().tickers())
                .containsExactly("TESTA", "FAKEB");
    }

    @Test
    void producerPayloadMatchesSchemaWithAndWithoutFilings() throws IOException {
        String one = serialize(new FilingsPayload(JOB_ID, 2, "TEST01", List.of(new FilingsPayload.Filing(
                "0000000001-26-000001", List.of("TESTA"), "6-K", List.of(), "2026-10-20T05:30:28+09:00",
                "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/test-6k.htm")), true));
        String none = serialize(new FilingsPayload(JOB_ID, 1, "TEST01", List.of(), false));

        assertThat(ContractSchemas.validate("news.filings.responded.v1.schema.json", one)).isEmpty();
        assertThat(ContractSchemas.validate("news.filings.responded.v1.schema.json", none)).isEmpty();
    }

    @Test
    void schemaRejectsRequestWithoutTickers() throws IOException {
        String noTickers = ContractSchemas.sample("news.filings.requested.sample.json")
                .replace("\"tickers\": [\"TESTA\", \"FAKEB\"]", "\"tickers\": []");

        assertThat(ContractSchemas.validate("news.filings.requested.v1.schema.json", noTickers)).isNotEmpty();
    }

    private String serialize(FilingsPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "news.filings.responded",
                OffsetDateTime.now(CLOCK), "collect-2026-10-20T16:00:30+09:00", payload));
    }
}

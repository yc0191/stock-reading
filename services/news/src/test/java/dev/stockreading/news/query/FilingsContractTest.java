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

/**
 * 내부 조회 API 의 요청·응답 본문은 옛 요청·응답 이벤트의 payload 계약(contracts/events/, 더 이상 쓰지 않는 토픽)과 같다.
 * 남겨 둔 샘플을 지금 요청 본문으로 계속 읽을 수 있고, 지금 응답 본문이 그 계약을 지키는지 본다(ADR-010).
 */
class FilingsContractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("news.filings.requested.sample.json");

        assertThat(ContractSchemas.validate("news.filings.requested.v1.schema.json", sample)).isEmpty();
        FilingsRequest request = payload(sample, FilingsRequest.class);
        assertThat(request.jobId()).isEqualTo(JOB_ID);
        assertThat(request.seq()).isEqualTo(1);
        assertThat(request.symbol()).isEqualTo("TEST01");
        assertThat(request.tickers()).containsExactly("TESTA", "FAKEB");
        assertThat(request.since()).isEqualTo("2026-10-17T16:00:31+09:00");
        assertThat(request.until()).isEqualTo("2026-10-20T16:00:31+09:00");
        assertThat(request.maxFilings()).isEqualTo(20);
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
        assertThat(payload(withExtraFields, FilingsRequest.class).tickers())
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

    /** 요청 이벤트 샘플의 payload 를 내부 조회 API 요청 본문으로 읽는다. 요청 본문은 그 payload 와 같은 모양이다. */
    private <T> T payload(String event, Class<T> type) {
        return mapper.treeToValue(mapper.readTree(event).get("payload"), type);
    }
}

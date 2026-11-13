package dev.stockreading.market.history;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.event.HistoryPayload;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 내부 조회 API 의 요청·응답 본문은 옛 요청·응답 이벤트의 payload 계약(contracts/events/, 더 이상 쓰지 않는 토픽)과 같다.
 * 남겨 둔 샘플을 지금 요청 본문으로 계속 읽을 수 있고, 지금 응답 본문이 그 계약을 지키는지 본다(ADR-010).
 */
class HistoryContractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:43Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("market.history.requested.sample.json");

        assertThat(ContractSchemas.validate("market.history.requested.v1.schema.json", sample)).isEmpty();
        HistoryRequest request = payload(sample, HistoryRequest.class);
        assertThat(request.jobId()).isEqualTo("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");
        assertThat(request.seq()).isEqualTo(1);
        assertThat(request.symbol()).isEqualTo("TEST01");
        assertThat(request.maxPoints()).isEqualTo(5000);
    }

    @Test
    void olderRequestWithoutIntervalIsReadAsNoThinning() throws IOException {
        String sample = ContractSchemas.sample("market.history.requested.sample.json");

        assertThat(payload(sample, HistoryRequest.class).intervalSeconds()).isNull();
    }

    @Test
    void requestWithIntervalMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String withInterval = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 60,\n    \"intervalSeconds\": 10");

        assertThat(ContractSchemas.validate("market.history.requested.v1.schema.json", withInterval)).isEmpty();
        assertThat(payload(withInterval, HistoryRequest.class).intervalSeconds())
                .isEqualTo(10);
        assertThat(ContractSchemas.validate("market.history.requested.v1.schema.json",
                withInterval.replace("\"intervalSeconds\": 10", "\"intervalSeconds\": 0"))).isNotEmpty();
    }

    @Test
    void responseSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate("market.history.responded.v1.schema.json",
                ContractSchemas.sample("market.history.responded.sample.json"))).isEmpty();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("market.history.requested.v1.schema.json", withExtraFields)).isEmpty();
        assertThat(payload(withExtraFields, HistoryRequest.class).symbol())
                .isEqualTo("TEST01");
    }

    @Test
    void answerPayloadMatchesTheResponseContractWithAndWithoutPoints() throws IOException {
        String one = serialize(new HistoryPayload(JOB_ID, 2, "TEST01",
                List.of(new HistoryPayload.Point("2026-10-20T16:00:31+09:00", 9_850L)), false));
        String none = serialize(new HistoryPayload(JOB_ID, 1, "TEST01", List.of(), false));

        assertThat(ContractSchemas.validate("market.history.responded.v1.schema.json", one)).isEmpty();
        assertThat(ContractSchemas.validate("market.history.responded.v1.schema.json", none)).isEmpty();
        assertThat(mapper.readTree(one).get("payload").get("seq").asInt()).isEqualTo(2);
    }

    // 응답 본문을 옛 응답 이벤트 봉투에 담아 계약 스키마로 검사한다.
    private String serialize(HistoryPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "market.history.responded",
                OffsetDateTime.now(CLOCK), "c", payload));
    }

    /** 요청 이벤트 샘플의 payload 를 내부 조회 API 요청 본문으로 읽는다. 요청 본문은 그 payload 와 같은 모양이다. */
    private <T> T payload(String event, Class<T> type) {
        return mapper.treeToValue(mapper.readTree(event).get("payload"), type);
    }
}

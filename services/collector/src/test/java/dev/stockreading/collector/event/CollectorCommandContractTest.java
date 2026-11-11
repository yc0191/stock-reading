package dev.stockreading.collector.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 사람이 보내는 수동 실행 명령을 수집기가 계속 읽을 수 있어야 한다(ADR-012). */
class CollectorCommandContractTest {

    private static final String SCHEMA = "collector.command.v1.schema.json";
    private static final String[] COMMANDS = {
            "collect", "collect-news", "collect-pre-open", "collect-intraday", "collect-post-close"};

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void committedSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, sample())).isEmpty();

        CommandEnvelope event = mapper.readValue(sample(), CommandEnvelope.class);
        assertThat(event.eventId()).isEqualTo("6f1d2c3b-4a59-4e7f-8d10-2b3c4d5e6f70");
        assertThat(event.eventType()).isEqualTo("collector.command");
        assertThat(event.payload().command()).isEqualTo("collect");
    }

    @Test
    void everyCollectionJobIsAnAllowedCommand() throws IOException {
        for (String command : COMMANDS) {
            String json = sample().replace("\"command\": \"collect\"", "\"command\": \"" + command + "\"");

            assertThat(ContractSchemas.validate(SCHEMA, json)).as(command).isEmpty();
        }
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String json = sample()
                .replace("\"correlationId\"", "\"futureEnvelopeField\": 1,\n  \"correlationId\"")
                .replace("\"command\"", "\"futurePayloadField\": {\"a\": 1},\n    \"command\"");

        assertThat(json).contains("futureEnvelopeField", "futurePayloadField");
        assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        assertThat(mapper.readValue(json, CommandEnvelope.class).payload().command()).isEqualTo("collect");
    }

    @Test
    void otherJobTypesAMissingCommandAndABadEventIdAreRejected() throws IOException {
        // 다른 서비스로 가는 tick 이름은 수집기 명령이 아니다.
        assertRejected(sample().replace("\"command\": \"collect\"", "\"command\": \"collect-members\""));
        assertRejected(sample().replace("\"command\": \"collect\"", "\"command\": \"daily-briefing-morning\""));
        assertRejected(sample().replace("\"command\": \"collect\"", "\"command\": \"\""));
        assertRejected(sample().replace("\"command\": \"collect\"", "\"jobType\": \"collect\""));
        assertRejected(sample().replace("6f1d2c3b-4a59-4e7f-8d10-2b3c4d5e6f70", "not-a-uuid"));
        assertRejected(sample().replace("\"eventType\": \"collector.command\"", "\"eventType\": \"schedule.tick\""));
    }

    private static String sample() throws IOException {
        return ContractSchemas.sample("collector.command.sample.json");
    }

    // 치환이 실제로 일어났는지 먼저 확인해야, 음성 테스트가 원본 샘플을 검증하며 통과하는 일이 없다.
    private void assertRejected(String json) throws IOException {
        assertThat(json).isNotEqualTo(sample());
        assertThat(ContractSchemas.validate(SCHEMA, json)).isNotEmpty();
    }
}

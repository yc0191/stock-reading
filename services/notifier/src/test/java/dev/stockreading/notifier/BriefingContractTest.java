package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dev.stockreading.notifier.event.BriefingEnvelope;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플을 현재 소비자가 계속 읽을 수 있어야 한다. */
class BriefingContractTest {

    @Test
    void v1SampleMatchesTheSchema() throws IOException {
        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", ContractSchemas.sample("briefing.generated.sample.json"))).isEmpty();
    }

    @Test
    void consumerReadsTheV1Sample() throws IOException {
        BriefingEnvelope briefing = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample("briefing.generated.sample.json"), BriefingEnvelope.class);

        assertThat(briefing.eventType()).isEqualTo("briefing.generated");
        assertThat(briefing.payload().symbol()).isEqualTo("TEST01");
        assertThat(briefing.payload().name()).isEqualTo("TEST 샘플 ETF");
        assertThat(briefing.payload().changePercent()).isEqualByComparingTo("-1.5");
    }

    @Test
    void sampleWithoutTheOptionalNameStillMatchesTheSchemaAndIsRead() throws IOException {
        String withoutName = ContractSchemas.sample("briefing.generated.sample.json")
                .replace("    \"name\": \"TEST 샘플 ETF\",\n", "");

        assertThat(withoutName).doesNotContain("\"name\"");
        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", withoutName)).isEmpty();
        assertThat(JsonMapper.builder().build().readValue(withoutName, BriefingEnvelope.class).payload().name()).isNull();
    }

    @Test
    void windowBriefingWithTodayChangeMatchesTheSchemaAndIsRead() throws IOException {
        String window = ContractSchemas.sample("briefing.generated.sample.json")
                .replace("\"payload\": {", "\"payload\": {\n    \"todayChangePercent\": -2.1,\n    \"moveMinutes\": 1,");

        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", window)).isEmpty();
        BriefingEnvelope.Payload payload = JsonMapper.builder().build().readValue(window, BriefingEnvelope.class).payload();
        assertThat(payload.todayChangePercent()).isEqualByComparingTo("-2.1");
        assertThat(payload.moveMinutes()).isEqualTo(1);
        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", window.replace("\"moveMinutes\": 1", "\"moveMinutes\": 0")))
                .isNotEmpty();
        // 칸이 없는 브리핑은 null로 읽혀 이전 제목을 쓴다.
        BriefingEnvelope older = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample("briefing.generated.sample.json"), BriefingEnvelope.class);
        assertThat(older.payload().todayChangePercent()).isNull();
        assertThat(older.payload().moveMinutes()).isNull();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtras = ContractSchemas.sample("briefing.generated.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", withExtras)).isEmpty();
        BriefingEnvelope briefing = JsonMapper.builder().build().readValue(withExtras, BriefingEnvelope.class);
        assertThat(briefing.payload().symbol()).isEqualTo("TEST01");
    }
}

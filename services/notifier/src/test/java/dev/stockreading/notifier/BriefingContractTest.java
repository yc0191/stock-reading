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
    void secondHighSampleMatchesTheSchemaAndIsRead() throws IOException {
        String high = ContractSchemas.sample("briefing.generated.high.sample.json");

        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", high)).isEmpty();
        BriefingEnvelope.Payload payload = JsonMapper.builder().build().readValue(high, BriefingEnvelope.class).payload();
        assertThat(payload.symbol()).isEqualTo("TEST01");
        assertThat(payload.todayChangePercent()).isEqualByComparingTo("-14.0");
        assertThat(payload.baseKind()).isEqualTo("earlierMove");
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
    void highBriefingWithBaseKindMatchesTheSchemaAndIsRead() throws IOException {
        String high = ContractSchemas.sample("briefing.generated.sample.json")
                .replace("\"payload\": {", "\"payload\": {\n    \"todayChangePercent\": -14.0,\n    \"baseKind\": \"earlierMove\",");

        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", high)).isEmpty();
        assertThat(JsonMapper.builder().build().readValue(high, BriefingEnvelope.class).payload().baseKind())
                .isEqualTo("earlierMove");
        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", high.replace("earlierMove", "other")))
                .isNotEmpty();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtras = ContractSchemas.sample("briefing.generated.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", withExtras)).isEmpty();
        BriefingEnvelope briefing = JsonMapper.builder().build().readValue(withExtras, BriefingEnvelope.class);
        assertThat(briefing.payload().symbol()).isEqualTo("TEST01");
    }

    private static final String OLD_FORMAT_TAIL = "(?s),\\s*\"html\".*?\\]\\s*(?=\\}\\s*\\})";

    @Test
    void consumerReadsTheOptionalHtmlAndImages() throws IOException {
        BriefingEnvelope.Payload payload = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample("briefing.generated.sample.json"), BriefingEnvelope.class).payload();

        assertThat(payload.html()).contains("cid:chart-1");
        assertThat(payload.images()).hasSize(1);
        assertThat(payload.images().get(0).contentId()).isEqualTo("chart-1");
        assertThat(payload.images().get(0).contentType()).isEqualTo("image/png");
    }

    @Test
    void olderEventWithoutHtmlAndImagesStillMatchesTheSchemaAndIsRead() throws IOException {
        String older = ContractSchemas.sample("briefing.generated.sample.json").replaceAll(OLD_FORMAT_TAIL, "\n  ");

        assertThat(older).doesNotContain("\"html\"").doesNotContain("\"images\"");
        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json", older)).isEmpty();
        BriefingEnvelope.Payload payload = JsonMapper.builder().build().readValue(older, BriefingEnvelope.class).payload();
        assertThat(payload.html()).isNull();
        assertThat(payload.images()).isNull();
    }

    @Test
    void emptyHtmlAndBadImagesAreRejectedBySchema() throws IOException {
        String sample = ContractSchemas.sample("briefing.generated.sample.json");

        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json",
                sample.replaceAll("\"html\": \"[^\n]*\",", "\"html\": \"\","))).isNotEmpty();
        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json",
                sample.replace("image/png", "image/svg+xml"))).isNotEmpty();
        assertThat(ContractSchemas.validate("briefing.generated.v1.schema.json",
                sample.replace("\"chart-1\"", "\"bad id\""))).isNotEmpty();
    }
}

package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dev.stockreading.notifier.event.DailyBriefingEnvelope;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 데일리 브리핑 샘플을 현재 소비자가 계속 읽을 수 있어야 한다. */
class DailyBriefingContractTest {

    private static final String SCHEMA = "briefing.daily.generated.v1.schema.json";
    private static final String SAMPLE = "briefing.daily.generated.sample.json";

    @Test
    void v1SampleMatchesTheSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, ContractSchemas.sample(SAMPLE))).isEmpty();
    }

    @Test
    void consumerReadsTheV1Sample() throws IOException {
        DailyBriefingEnvelope daily = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample(SAMPLE), DailyBriefingEnvelope.class);

        assertThat(daily.eventType()).isEqualTo("briefing.daily.generated");
        assertThat(daily.payload().edition()).isEqualTo("post-close");
        assertThat(daily.payload().rangeStart()).isEqualTo("2026-10-19T17:00:00+09:00");
        assertThat(daily.payload().rangeEnd()).isEqualTo("2026-10-20T17:00:00+09:00");
        assertThat(daily.payload().text()).contains("자료 없음");
        assertThat(daily.payload().missingItems()).containsExactly("etf-daily-change:TEST02");
        assertThat(daily.payload().generatedBy()).isEqualTo("llm");
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtras = ContractSchemas.sample(SAMPLE)
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate(SCHEMA, withExtras)).isEmpty();
        DailyBriefingEnvelope daily = JsonMapper.builder().build().readValue(withExtras, DailyBriefingEnvelope.class);
        assertThat(daily.payload().edition()).isEqualTo("post-close");
    }

    @Test
    void unknownEditionIsRejectedByTheSchema() throws IOException {
        String broken = ContractSchemas.sample(SAMPLE).replace("\"edition\": \"post-close\"", "\"edition\": \"noon\"");

        assertThat(ContractSchemas.validate(SCHEMA, broken)).isNotEmpty();
    }

    @Test
    void rangeTimeWithoutSecondsIsRejectedProvingFormatChecksAreOn() throws IOException {
        String broken = ContractSchemas.sample(SAMPLE)
                .replace("\"rangeStart\": \"2026-10-19T17:00:00+09:00\"", "\"rangeStart\": \"2026-10-19T17:00+09:00\"");

        assertThat(ContractSchemas.validate(SCHEMA, broken)).isNotEmpty();
    }

    @Test
    void consumerReadsTheOptionalHtmlAndImages() throws IOException {
        DailyBriefingEnvelope daily = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample(SAMPLE), DailyBriefingEnvelope.class);

        assertThat(daily.payload().html()).contains("cid:chart-1");
        assertThat(daily.payload().images()).hasSize(1);
        assertThat(daily.payload().images().get(0).contentId()).isEqualTo("chart-1");
        assertThat(daily.payload().images().get(0).contentType()).isEqualTo("image/png");
    }

    @Test
    void sampleWithoutHtmlAndImagesStillMatchesAndIsReadable() throws IOException {
        String textOnly = ContractSchemas.sample(SAMPLE).replaceAll("(?s),\\s*\"html\".*?\\]\\s*(?=\\})", "\n  ");

        assertThat(textOnly).doesNotContain("\"html\"");
        assertThat(ContractSchemas.validate(SCHEMA, textOnly)).isEmpty();
        DailyBriefingEnvelope daily = JsonMapper.builder().build().readValue(textOnly, DailyBriefingEnvelope.class);
        assertThat(daily.payload().html()).isNull();
        assertThat(daily.payload().images()).isNull();
    }

    @Test
    void imageWithAnUnknownTypeOrNonBase64DataIsRejected() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA,
                ContractSchemas.sample(SAMPLE).replace("image/png", "image/svg+xml"))).isNotEmpty();
        assertThat(ContractSchemas.validate(SCHEMA,
                ContractSchemas.sample(SAMPLE).replace("iVBORw0KGgo", "iVBORw0K Ggo"))).isNotEmpty();
    }
}

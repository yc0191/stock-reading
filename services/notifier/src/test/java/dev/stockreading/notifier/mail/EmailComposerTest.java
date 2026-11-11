package dev.stockreading.notifier.mail;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.notifier.NotifierProperties;
import dev.stockreading.notifier.event.BriefingEnvelope;
import dev.stockreading.notifier.event.BriefingEnvelope.Evidence;
import dev.stockreading.notifier.event.BriefingEnvelope.Payload;

class EmailComposerTest {

    private static BriefingEnvelope briefing(String change) {
        return briefing(change, null);
    }

    private static BriefingEnvelope briefing(String change, String name) {
        Payload payload = new Payload(
                "TEST01", name, new BigDecimal(change), "2026-10-20T16:00:31+09:00",
                "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)\n근거 후보:\n  1. Sample headline — sample",
                "rule-based",
                List.of(new Evidence("Sample headline", "sample", "2026-10-20T09:00:00+09:00")));
        return new BriefingEnvelope(
                "9b7d1f20-3a4c-4e8b-b5d6-2c1a0f9e8d77", "briefing.generated", 1,
                "2026-10-20T16:00:31+09:00", "2026-10-20T16:00:32+09:00",
                "collect-2026-10-20T16:00:30+09:00", payload);
    }

    private static EmailComposer composer(String prefix) {
        return new EmailComposer(new NotifierProperties(false, "from@example.com", prefix));
    }

    @Test
    void subjectShowsPrefixSymbolDirectionAndSignedPercent() {
        assertThat(composer("[시뮬레이션]").compose(briefing("-1.50")).subject())
                .isEqualTo("[시뮬레이션] [TEST01] 하락 -1.50% 브리핑");
        assertThat(composer("[시뮬레이션]").compose(briefing("2.37")).subject())
                .isEqualTo("[시뮬레이션] [TEST01] 상승 +2.37% 브리핑");
    }

    @Test
    void subjectShowsNameWithSymbolWhenNameIsKnown() {
        assertThat(composer("").compose(briefing("-1.34", "TEST 샘플 ETF")).subject())
                .isEqualTo("[TEST 샘플 ETF(TEST01)] 하락 -1.34% 브리핑");
        assertThat(composer("").compose(briefing("-1.34", "  TEST 샘플 ETF ")).subject())
                .isEqualTo("[TEST 샘플 ETF(TEST01)] 하락 -1.34% 브리핑");
    }

    @Test
    void missingOrBlankNameFallsBackToSymbolOnly() {
        assertThat(composer("").compose(briefing("-1.34", null)).subject()).isEqualTo("[TEST01] 하락 -1.34% 브리핑");
        assertThat(composer("").compose(briefing("-1.34", "")).subject()).isEqualTo("[TEST01] 하락 -1.34% 브리핑");
        assertThat(composer("").compose(briefing("-1.34", "   ")).subject()).isEqualTo("[TEST01] 하락 -1.34% 브리핑");
    }

    @Test
    void blankOrMissingPrefixLeavesNoStrayWhitespace() {
        assertThat(composer("").compose(briefing("-1.50")).subject()).isEqualTo("[TEST01] 하락 -1.50% 브리핑");
        assertThat(composer("   ").compose(briefing("-1.50")).subject()).isEqualTo("[TEST01] 하락 -1.50% 브리핑");
        assertThat(composer(null).compose(briefing("-1.50")).subject()).isEqualTo("[TEST01] 하락 -1.50% 브리핑");
    }

    @Test
    void bodyKeepsTheBriefingTextAndAddsTraceIds() {
        String body = composer("").compose(briefing("-1.50")).body();

        assertThat(body).startsWith("[TEST01] 16:00 기준 하락 -1.50%");
        assertThat(body).contains("근거 후보:");
        assertThat(body).contains("브리핑 ID: 9b7d1f20-3a4c-4e8b-b5d6-2c1a0f9e8d77");
        assertThat(body).contains("추적 ID: collect-2026-10-20T16:00:30+09:00");
        assertThat(body).contains("생성 방식: rule-based");
    }

    @Test
    void theEmailNeverContainsTheConfiguredAddresses() {
        EmailMessage email = composer("[시뮬레이션]").compose(briefing("-1.50"));

        assertThat(email.subject() + email.body()).doesNotContain("to@example.com").doesNotContain("from@example.com");
    }

    @Test
    void committedSampleBriefingCanBeComposed() throws IOException {
        var mapper = tools.jackson.databind.json.JsonMapper.builder().build();
        BriefingEnvelope sample = mapper.readValue(
                dev.stockreading.notifier.ContractSchemas.sample("briefing.generated.sample.json"), BriefingEnvelope.class);

        EmailMessage email = composer("[시뮬레이션]").compose(sample);

        assertThat(email.subject()).isEqualTo("[시뮬레이션] [TEST 샘플 ETF(TEST01)] 하락 -1.50% 브리핑");
        assertThat(email.body()).startsWith("[TEST 샘플 ETF(TEST01)] 16:00");
        assertThat(email.body()).contains("근거 후보");
    }
}

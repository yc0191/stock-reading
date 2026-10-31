package dev.stockreading.notifier.mail;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.notifier.ContractSchemas;
import dev.stockreading.notifier.NotifierProperties;
import dev.stockreading.notifier.event.DailyBriefingEnvelope;
import dev.stockreading.notifier.event.DailyBriefingEnvelope.Payload;
import tools.jackson.databind.json.JsonMapper;

class DailyEmailComposerTest {

    private static DailyBriefingEnvelope daily(String edition, String rangeEnd, List<String> missingItems) {
        Payload payload = new Payload(
                edition, "2026-10-19T08:45:00+09:00", rangeEnd,
                "[아침 판] 10-20\n\n해외 시장 요약: 샘플 지수 +0.40%\n밤사이 미국 공시: 자료 없음",
                missingItems, "llm");
        return new DailyBriefingEnvelope(
                "5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40", "briefing.daily.generated", 1,
                rangeEnd, "2026-10-20T08:45:41+09:00", "daily-briefing-" + edition + "-" + rangeEnd, payload);
    }

    private static DailyEmailComposer composer(String prefix) {
        return new DailyEmailComposer(new NotifierProperties(false, "from@example.com", "to@example.com", prefix, 6));
    }

    @Test
    void subjectShowsPrefixEditionAndDate() {
        assertThat(composer("[시뮬레이션]").compose(daily("morning", "2026-10-20T08:45:00+09:00", List.of())).subject())
                .isEqualTo("[시뮬레이션] 아침 판 데일리 브리핑 2026-10-20");
        assertThat(composer("[시뮬레이션]").compose(daily("post-close", "2026-10-20T17:00:00+09:00", List.of())).subject())
                .isEqualTo("[시뮬레이션] 마감 후 판 데일리 브리핑 2026-10-20");
    }

    @Test
    void dateIsTheKoreanDateOfTheSendTimeNotTheUtcDate() {
        // 한국 시간 08:45는 UTC로 전날 23:45다. 제목은 한국 날짜를 써야 한다.
        assertThat(composer("").compose(daily("morning", "2026-10-20T08:45:00+09:00", List.of())).subject())
                .isEqualTo("아침 판 데일리 브리핑 2026-10-20");
    }

    @Test
    void blankOrMissingPrefixLeavesNoStrayWhitespace() {
        DailyBriefingEnvelope morning = daily("morning", "2026-10-20T08:45:00+09:00", List.of());

        assertThat(composer("").compose(morning).subject()).isEqualTo("아침 판 데일리 브리핑 2026-10-20");
        assertThat(composer("   ").compose(morning).subject()).isEqualTo("아침 판 데일리 브리핑 2026-10-20");
        assertThat(composer(null).compose(morning).subject()).isEqualTo("아침 판 데일리 브리핑 2026-10-20");
    }

    @Test
    void anUnknownEditionStillProducesAnEmailWithTheRawValue() {
        assertThat(composer("").compose(daily("noon", "2026-10-20T12:00:00+09:00", List.of())).subject())
                .isEqualTo("noon 데일리 브리핑 2026-10-20");
    }

    @Test
    void bodyKeepsTheTextAndListsEachMissingItem() {
        String body = composer("").compose(daily("morning", "2026-10-20T08:45:00+09:00",
                List.of("us-filings", "etf-daily-change:TEST02"))).body();

        assertThat(body).startsWith("[아침 판] 10-20\n\n해외 시장 요약");
        assertThat(body).contains("밤사이 미국 공시: 자료 없음");
        assertThat(body).contains("자료 없음 항목: us-filings, etf-daily-change:TEST02");
        assertThat(body).contains("다룬 범위: 2026-10-19T08:45:00+09:00 ~ 2026-10-20T08:45:00+09:00");
        assertThat(body).contains("브리핑 ID: 5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40");
        assertThat(body).contains("추적 ID: daily-briefing-morning-2026-10-20T08:45:00+09:00");
        assertThat(body).contains("생성 방식: llm");
    }

    @Test
    void noMissingItemsIsShownAsNone() {
        assertThat(composer("").compose(daily("morning", "2026-10-20T08:45:00+09:00", List.of())).body())
                .contains("자료 없음 항목: 없음");
        assertThat(composer("").compose(daily("morning", "2026-10-20T08:45:00+09:00", null)).body())
                .contains("자료 없음 항목: 없음");
    }

    @Test
    void anInvalidSendTimeIsRejected() {
        assertThatThrownBy(() -> composer("").compose(daily("morning", "yesterday", List.of())))
                .isInstanceOf(DateTimeParseException.class);
    }

    @Test
    void theEmailNeverContainsTheConfiguredAddresses() {
        EmailMessage email = composer("[시뮬레이션]").compose(daily("morning", "2026-10-20T08:45:00+09:00", List.of()));

        assertThat(email.subject() + email.body()).doesNotContain("to@example.com").doesNotContain("from@example.com");
    }

    @Test
    void committedSampleCanBeComposed() throws IOException {
        DailyBriefingEnvelope sample = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample("briefing.daily.generated.sample.json"), DailyBriefingEnvelope.class);

        EmailMessage email = composer("[시뮬레이션]").compose(sample);

        assertThat(email.subject()).isEqualTo("[시뮬레이션] 마감 후 판 데일리 브리핑 2026-10-20");
        assertThat(email.body()).contains("TEST02 자료 없음").contains("자료 없음 항목: etf-daily-change:TEST02");
    }
}

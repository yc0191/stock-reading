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
    void moveMailIsSentUnderItsOwnSenderName() {
        assertThat(composer("").compose(briefing("-1.50")).fromName()).isEqualTo("변동 감지");
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

    /** 짧은 시간 변동 브리핑. todayChangePercent·moveMinutes가 있으면 제목에 오늘 등락을 먼저 쓴다(#66). */
    private static BriefingEnvelope windowBriefing(String name, String change, String today, String minutes) {
        return windowBriefing(name, change, today, minutes, null);
    }

    private static BriefingEnvelope windowBriefing(String name, String change, String today, String minutes,
            String detectedAt) {
        String extras = (today == null ? "" : ", \"todayChangePercent\": " + today)
                + (detectedAt == null ? "" : ", \"detectedAt\": \"" + detectedAt + "\"")
                + (minutes == null ? "" : ", \"moveMinutes\": " + minutes)
                + (name == null ? "" : ", \"name\": \"" + name + "\"");
        String json = """
                {"eventId": "9b7d1f20-3a4c-4e8b-b5d6-2c1a0f9e8d77", "eventType": "briefing.generated", "schemaVersion": 1,
                 "occurredAt": "2026-10-20T14:39:00+09:00", "producedAt": "2026-10-20T14:45:00+09:00",
                 "correlationId": "collect-2026-10-20T14:40:00+09:00",
                 "payload": {"symbol": "TEST01", "changePercent": %s, "asOf": "2026-10-20T05:39:00Z",
                             "text": "본문", "generatedBy": "rule-based", "evidence": []%s}}
                """.formatted(change, extras);
        return tools.jackson.databind.json.JsonMapper.builder().build().readValue(json, BriefingEnvelope.class);
    }

    @Test
    void windowSubjectShowsTodayChangeFirstInKoreanTime() {
        // 오늘 등락과 짧은 변동 방향이 반대면 반등/반락이다. 시각은 asOf를 한국 시간으로 바꿔 쓴다.
        assertThat(composer("").compose(windowBriefing("TEST 샘플 ETF", "1.00", "-2.00", "1")).subject())
                .isEqualTo("[TEST 샘플 ETF(TEST01)] 14:39 기준 오늘 -2.00% · 1분 새 +1.00% 반등 브리핑");
        assertThat(composer("[시뮬레이션]").compose(windowBriefing(null, "-1.00", "1.20", "3")).subject())
                .isEqualTo("[시뮬레이션] [TEST01] 14:39 기준 오늘 +1.20% · 3분 새 -1.00% 반락 브리핑");
    }

    /** 레벨 상 브리핑. baseKind가 있으면 제목에 오늘 등락을 먼저 쓰고, 두 번째 상부터 기준가 대비를 붙인다. */
    private static BriefingEnvelope highBriefing(String change, String today, String baseKind) {
        BriefingEnvelope base = windowBriefing("TEST 샘플 ETF", change, today, null);
        Payload p = base.payload();
        return new BriefingEnvelope(base.eventId(), base.eventType(), 1, base.occurredAt(), base.producedAt(),
                base.correlationId(), new Payload(p.symbol(), p.name(), p.changePercent(), p.asOf(), p.text(),
                        p.generatedBy(), p.evidence(), p.todayChangePercent(), null, null, null, baseKind));
    }

    @Test
    void highSubjectShowsTodayChangeAndAddsTheBaseChangeOnlyFromTheSecondHighMove() {
        assertThat(composer("").compose(highBriefing("-7.20", "-7.20", "prevClose")).subject())
                .isEqualTo("[TEST 샘플 ETF(TEST01)] 14:39 기준 오늘 -7.20% 하락 브리핑");
        assertThat(composer("").compose(highBriefing("-7.53", "-14.00", "earlierMove")).subject())
                .isEqualTo("[TEST 샘플 ETF(TEST01)] 14:39 기준 오늘 -14.00% · 기준가 대비 -7.53% 하락 브리핑");
        assertThat(composer("").compose(highBriefing("7.10", "-2.00", "earlierMove")).subject())
                .isEqualTo("[TEST 샘플 ETF(TEST01)] 14:39 기준 오늘 -2.00% · 기준가 대비 +7.10% 반등 브리핑");
    }

    @Test
    void windowSubjectSaysRiseOrFallWhenTodayChangeIsTheSameWayOrFlat() {
        assertThat(composer("").compose(windowBriefing(null, "-1.00", "-2.00", "1")).subject())
                .isEqualTo("[TEST01] 14:39 기준 오늘 -2.00% · 1분 새 -1.00% 하락 브리핑");
        assertThat(composer("").compose(windowBriefing(null, "1.00", "0.00", "2")).subject())
                .isEqualTo("[TEST01] 14:39 기준 오늘 +0.00% · 2분 새 +1.00% 상승 브리핑");
    }

    @Test
    void briefingWithoutTodayChangeKeepsThePreviousSubject() {
        assertThat(composer("").compose(windowBriefing(null, "1.00", null, null)).subject())
                .isEqualTo("[TEST01] 상승 +1.00% 브리핑");
    }

    @Test
    void windowSubjectSaysWhenTheTodayChangeWasReadInKoreanTime() {
        // 앞의 시각은 움직임이 시작된 때이고 오늘 등락은 감지 때 값이라, 브리핑이 감지 시각을 주면 본문 첫 줄처럼 밝힌다.
        assertThat(composer("").compose(windowBriefing(null, "1.00", "-2.00", "3", "2026-10-20T05:42:05Z")).subject())
                .isEqualTo("[TEST01] 14:39 기준 오늘 -2.00%(14:42 감지 기준) · 3분 새 +1.00% 반등 브리핑");
        // 읽을 수 없는 감지 시각은 붙이지 않는다(제목은 그대로 나간다).
        assertThat(composer("").compose(windowBriefing(null, "1.00", "-2.00", "3", "not-a-time")).subject())
                .isEqualTo("[TEST01] 14:39 기준 오늘 -2.00% · 3분 새 +1.00% 반등 브리핑");
    }

    @Test
    void windowSubjectWithoutMinutesSaysShortTime() {
        // 다시 보낸 변동이라 브리핑이 분을 뺐으면 'N분 새' 대신 '짧은 시간'으로 쓴다.
        assertThat(composer("").compose(windowBriefing(null, "1.00", "-2.00", null)).subject())
                .isEqualTo("[TEST01] 14:39 기준 오늘 -2.00% · 짧은 시간 +1.00% 반등 브리핑");
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

    private static final String PNG_1X1 =
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";

    private static BriefingEnvelope withHtml(String html, List<BriefingEnvelope.Image> images) {
        Payload p = briefing("-1.50", "TEST 샘플 ETF").payload();
        Payload payload = new Payload(p.symbol(), p.name(), p.changePercent(), p.asOf(), p.text(), p.generatedBy(),
                p.evidence(), null, null, html, images);
        BriefingEnvelope base = briefing("-1.50");
        return new BriefingEnvelope(base.eventId(), base.eventType(), base.schemaVersion(), base.occurredAt(),
                base.producedAt(), base.correlationId(), payload);
    }

    @Test
    void withoutHtmlTheMailStaysTextOnly() {
        EmailMessage email = composer("").compose(briefing("-1.50"));

        assertThat(email.hasHtml()).isFalse();
        assertThat(email.images()).isEmpty();
        assertThat(email.fromName()).isEqualTo("변동 감지");
        // 빈 html도 없는 것과 같다(이전 형식과 같은 글자판 메일).
        assertThat(composer("").compose(withHtml("  ", List.of(new BriefingEnvelope.Image("c1", "image/png", PNG_1X1))))
                .hasHtml()).isFalse();
    }

    @Test
    void htmlMailKeepsTheTextAndSubjectAndGetsTheFooterAndImages() {
        BriefingEnvelope briefing = withHtml("<html><body><p>안녕</p><img src=\"cid:c1\"></body></html>",
                List.of(new BriefingEnvelope.Image("c1", "image/png", PNG_1X1)));

        EmailMessage email = composer("[시뮬레이션]").compose(briefing);

        assertThat(email.subject()).isEqualTo("[시뮬레이션] [TEST 샘플 ETF(TEST01)] 하락 -1.50% 브리핑");
        assertThat(email.fromName()).isEqualTo("변동 감지");
        assertThat(email.body()).startsWith("[TEST01] 16:00 기준 하락 -1.50%").contains("생성 방식: rule-based");
        assertThat(email.hasHtml()).isTrue();
        assertThat(email.html()).startsWith("<html><body><p>안녕</p>").endsWith("</div></body></html>")
                .contains("브리핑 ID: 9b7d1f20-3a4c-4e8b-b5d6-2c1a0f9e8d77")
                .contains("추적 ID: collect-2026-10-20T16:00:30+09:00")
                .contains("생성 방식: rule-based");
        assertThat(email.images()).hasSize(1);
        assertThat(email.images().get(0).contentId()).isEqualTo("c1");
        assertThat(email.images().get(0).data()).startsWith((byte) 0x89, (byte) 'P', (byte) 'N', (byte) 'G');
    }

    @Test
    void invalidImagesAreDroppedAndTheHtmlMailStillGoesOut() {
        List<BriefingEnvelope.Image> images = List.of(
                new BriefingEnvelope.Image("good", "image/png", PNG_1X1),
                new BriefingEnvelope.Image("broken", "image/png", "!!not base64!!"),
                new BriefingEnvelope.Image("badtype", "image/svg+xml", PNG_1X1),
                new BriefingEnvelope.Image("bad id", "image/png", PNG_1X1));

        EmailMessage email = composer("").compose(withHtml("<p>x</p>", images));

        assertThat(email.hasHtml()).isTrue();
        assertThat(email.images()).extracting(EmailMessage.InlineImage::contentId).containsExactly("good");
    }

    @Test
    void footerValuesAreHtmlEscaped() {
        BriefingEnvelope base = withHtml("<p>x</p>", null);
        Payload p = base.payload();
        Payload payload = new Payload(p.symbol(), p.name(), p.changePercent(), p.asOf(), p.text(), "a&b<i>",
                p.evidence(), null, null, p.html(), null);
        BriefingEnvelope hostile = new BriefingEnvelope(base.eventId(), base.eventType(), 1, base.occurredAt(),
                base.producedAt(), base.correlationId(), payload);

        String html = composer("").compose(hostile).html();

        assertThat(html).doesNotContain("<i>").contains("a&amp;b&lt;i&gt;");
    }

    @Test
    void committedSampleCarriesHtmlAndImagesIntoTheMail() throws IOException {
        BriefingEnvelope sample = tools.jackson.databind.json.JsonMapper.builder().build().readValue(
                dev.stockreading.notifier.ContractSchemas.sample("briefing.generated.sample.json"), BriefingEnvelope.class);

        EmailMessage email = composer("").compose(sample);

        assertThat(email.hasHtml()).isTrue();
        assertThat(email.html()).contains("cid:chart-1");
        assertThat(email.images()).extracting(EmailMessage.InlineImage::contentId).containsExactly("chart-1");
    }
}

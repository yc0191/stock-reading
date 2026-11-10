package dev.stockreading.notifier.mail;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import dev.stockreading.notifier.ContractSchemas;
import dev.stockreading.notifier.NotifierProperties;
import dev.stockreading.notifier.event.DailyBriefingEnvelope;
import dev.stockreading.notifier.event.DailyBriefingEnvelope.Image;
import dev.stockreading.notifier.event.DailyBriefingEnvelope.Payload;
import tools.jackson.databind.json.JsonMapper;

class DailyEmailComposerTest {

    private ListAppender<ILoggingEvent> logs;

    @BeforeEach
    void attachLogs() {
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(DailyEmailComposer.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(DailyEmailComposer.class)).detachAppender(logs);
    }

    private static DailyBriefingEnvelope daily(String edition, String rangeEnd, List<String> missingItems) {
        Payload payload = new Payload(
                edition, "2026-10-19T08:45:00+09:00", rangeEnd,
                "[아침 판] 10-20\n\n해외 시장 요약: 샘플 지수 +0.40%\n밤사이 미국 공시: 자료 없음",
                missingItems, "llm", null, null);
        return new DailyBriefingEnvelope(
                "5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40", "briefing.daily.generated", 1,
                rangeEnd, "2026-10-20T08:45:41+09:00", "daily-briefing-" + edition + "-" + rangeEnd, payload);
    }

    private static DailyEmailComposer composer(String prefix) {
        return new DailyEmailComposer(new NotifierProperties(false, "from@example.com", prefix, 6));
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

    private static DailyBriefingEnvelope withHtml(String html, List<Image> images) {
        DailyBriefingEnvelope base = daily("morning", "2026-10-20T08:45:00+09:00", List.of("us-filings"));
        Payload p = base.payload();
        Payload payload = new Payload(p.edition(), p.rangeStart(), p.rangeEnd(), p.text(), p.missingItems(),
                p.generatedBy(), html, images);
        return new DailyBriefingEnvelope(base.eventId(), base.eventType(), base.schemaVersion(), base.occurredAt(),
                base.producedAt(), base.correlationId(), payload);
    }

    private static final String PNG_1X1 =
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";

    @Test
    void withoutHtmlTheMailStaysTextOnly() {
        EmailMessage email = composer("").compose(daily("morning", "2026-10-20T08:45:00+09:00", List.of()));

        assertThat(email.hasHtml()).isFalse();
        assertThat(email.images()).isEmpty();
    }

    @Test
    void htmlGetsTheSameFooterBeforeTheClosingBodyTagAndImagesAreDecoded() {
        EmailMessage email = composer("").compose(withHtml("<html><body><p>안녕</p><img src=\"cid:c1\"></body></html>",
                List.of(new Image("c1", "image/png", PNG_1X1))));

        assertThat(email.hasHtml()).isTrue();
        assertThat(email.html()).startsWith("<html><body><p>안녕</p>").endsWith("</div></body></html>");
        assertThat(email.html()).contains("자료 없음 항목: us-filings")
                .contains("다룬 범위: 2026-10-19T08:45:00+09:00 ~ 2026-10-20T08:45:00+09:00")
                .contains("브리핑 ID: 5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40")
                .contains("추적 ID: daily-briefing-morning-2026-10-20T08:45:00+09:00")
                .contains("생성 방식: llm");
        assertThat(email.images()).hasSize(1);
        assertThat(email.images().get(0).contentId()).isEqualTo("c1");
        assertThat(email.images().get(0).data()).startsWith((byte) 0x89, (byte) 'P', (byte) 'N', (byte) 'G');
        assertThat(email.body()).contains("자료 없음 항목: us-filings"); // 글자판 꼬리는 그대로
    }

    @Test
    void htmlWithoutABodyTagGetsTheFooterAppended() {
        EmailMessage email = composer("").compose(withHtml("<p>조각</p>", null));

        assertThat(email.html()).startsWith("<p>조각</p><div").contains("브리핑 ID: ");
        assertThat(email.images()).isEmpty();
    }

    @Test
    void footerValuesAreHtmlEscaped() {
        DailyBriefingEnvelope base = withHtml("<p>x</p>", null);
        Payload p = base.payload();
        Payload payload = new Payload(p.edition(), p.rangeStart(), p.rangeEnd(), p.text(),
                List.of("<script>alert(1)</script>"), "a&b", p.html(), null);
        DailyBriefingEnvelope hostile = new DailyBriefingEnvelope(base.eventId(), base.eventType(), 1,
                base.occurredAt(), base.producedAt(), base.correlationId(), payload);

        String html = composer("").compose(hostile).html();

        assertThat(html).doesNotContain("<script>").contains("&lt;script&gt;alert(1)&lt;/script&gt;").contains("a&amp;b");
    }

    @Test
    void invalidImagesAreDroppedOneByOneAndTheHtmlMailStillGoesOut() {
        List<Image> images = List.of(
                new Image("good", "image/png", PNG_1X1),
                new Image("broken", "image/png", "!!not base64!!"),
                new Image("badtype", "image/svg+xml", PNG_1X1),
                new Image("bad id", "image/png", PNG_1X1),
                new Image("bad<id>", "image/png", PNG_1X1),
                new Image("bad\r\nid", "image/png", PNG_1X1),
                new Image("empty", "image/png", ""));

        EmailMessage email = composer("").compose(withHtml("<p>x</p>", images));

        assertThat(email.hasHtml()).isTrue();
        assertThat(email.images()).extracting(EmailMessage.InlineImage::contentId).containsExactly("good");
        assertThat(email.html()).doesNotContain("차트 없음");
    }

    @Test
    void whenEveryImageIsInvalidTheHtmlMailStillGoesOutWithoutImages() {
        EmailMessage email = composer("").compose(withHtml("<p>x</p>", List.of(new Image("c1", "image/png", "!!"))));

        assertThat(email.hasHtml()).isTrue();
        assertThat(email.images()).isEmpty();
    }

    @Test
    void footerIsPlacedBeforeTheLastClosingBodyTagIgnoringCaseEvenAfterALengthChangingCharacter() {
        // U+0130(İ)는 소문자로 바꾸면 길이가 달라져 위치가 어긋난다
        EmailMessage email = composer("").compose(withHtml("<html><body><p>İİİ</p></BODY ></html>", null));

        assertThat(email.html()).startsWith("<html><body><p>İİİ</p><div").endsWith("</div></BODY ></html>");
    }

    @Test
    void footerGoesBeforeTheLastClosingBodyTagWhenTheHtmlHasTwo() {
        EmailMessage email = composer("").compose(withHtml("<body><pre></body></pre></body></html>", null));

        assertThat(email.html()).startsWith("<body><pre></body></pre><div").endsWith("</div></body></html>");
    }

    @Test
    void droppingInvalidImagesLogsOneWarnWithCountsButNoValues() {
        List<Image> images = List.of(
                new Image("good", "image/png", PNG_1X1),
                new Image("secretId", "image/svg+xml", PNG_1X1),
                new Image("bad id", "image/png", PNG_1X1),
                new Image("brokenId", "image/png", "!!zzzBase64Piece!!"));

        composer("").compose(withHtml("<p>x</p>", images));

        assertThat(logs.list).hasSize(1);
        ILoggingEvent event = logs.list.get(0);
        assertThat(event.getLevel()).isEqualTo(Level.WARN);
        assertThat(event.getFormattedMessage())
                .contains("eventId=5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40")
                .contains("badFormat=1").contains("badContentId=1").contains("badData=1")
                .doesNotContain("zzzBase64Piece").doesNotContain("iVBORw0KGgo")
                .doesNotContain("secretId").doesNotContain("brokenId").doesNotContain("bad id")
                .doesNotContain("@example.com");
    }

    @Test
    void committedSampleWithHtmlComposesIntoAMultipartMessage() throws IOException {
        DailyBriefingEnvelope sample = JsonMapper.builder().build()
                .readValue(ContractSchemas.sample("briefing.daily.generated.sample.json"), DailyBriefingEnvelope.class);

        EmailMessage email = composer("").compose(sample);

        assertThat(email.hasHtml()).isTrue();
        assertThat(email.images()).extracting(EmailMessage.InlineImage::contentId).containsExactly("chart-1");
    }
}

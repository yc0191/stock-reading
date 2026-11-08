package dev.stockreading.notifier.mail;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import com.icegreen.greenmail.junit5.GreenMailExtension;
import com.icegreen.greenmail.util.ServerSetupTest;

import dev.stockreading.notifier.NotifierProperties;
import jakarta.mail.internet.MimeMessage;

/** 프로세스 안의 테스트용 서버와 실제 SMTP로 통신해서, 한글의 MIME 인코딩이 실제로 동작하는지 확인한다. */
class SmtpMailSenderTest {

    @RegisterExtension
    static final GreenMailExtension GREEN_MAIL = new GreenMailExtension(ServerSetupTest.SMTP);

    private static JavaMailSenderImpl smtpClient() {
        JavaMailSenderImpl client = new JavaMailSenderImpl();
        client.setHost("localhost");
        client.setPort(ServerSetupTest.SMTP.getPort());
        return client;
    }

    private static NotifierProperties properties(String sender, String recipient) {
        return new NotifierProperties(true, sender, recipient, "", 6);
    }

    @Test
    void deliversKoreanSubjectAndBodyIntact() throws Exception {
        var sender = new SmtpMailSender(smtpClient(), properties("from@example.com", "to@example.com"));
        var email = new EmailMessage(
                "[시뮬레이션] [TEST01] 하락 -1.50% 브리핑",
                "[TEST01] 16:00 기준 하락 -1.50% (10,000 → 9,850)\n※ 투자 조언이 아닙니다.");

        sender.send(email);

        MimeMessage[] received = GREEN_MAIL.getReceivedMessages();
        assertThat(received).hasSize(1);
        assertThat(received[0].getSubject()).isEqualTo(email.subject());
        assertThat(((String) received[0].getContent()).strip()).isEqualTo(email.body());
        assertThat(received[0].getFrom()[0].toString()).isEqualTo("from@example.com");
        assertThat(received[0].getAllRecipients()[0].toString()).isEqualTo("to@example.com");
    }

    @Test
    void refusesToStartWithoutARecipientOrSender() {
        assertThatThrownBy(() -> new SmtpMailSender(smtpClient(), properties("from@example.com", "")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("recipient");
        assertThatThrownBy(() -> new SmtpMailSender(smtpClient(), properties(null, "to@example.com")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("sender");
    }

    @Test
    void aServerErrorThatMentionsOurAddressAndAccountNumberNeverReachesTheLogs() {
        JavaMailSenderImpl failing = new JavaMailSenderImpl() {
            @Override
            public void send(MimeMessage mimeMessage) {
                throw new org.springframework.mail.MailSendException(
                        "554 Access denied: User 'arn:aws:iam::123456789012:user/x' is not authorized to perform "
                                + "'ses:SendRawEmail' on 'arn:aws:ses:ap-southeast-2:123456789012:identity/to@example.com'");
            }
        };
        failing.setHost("localhost");
        var sender = new SmtpMailSender(failing, properties("from@example.com", "to@example.com"));

        assertThatThrownBy(() -> sender.send(new EmailMessage("subject", "body")))
                .isInstanceOf(MailDeliveryException.class)
                .hasMessageContaining("554 Access denied")
                .hasMessageNotContaining("to@example.com")
                .hasMessageNotContaining("123456789012")
                .hasNoCause(); // 원인을 연결하면 마스킹되지 않은 원문과 함께 로그에 찍힌다
    }

    @Test
    void refusesToStartWithoutAnSmtpHost() {
        var noHost = new JavaMailSenderImpl();

        assertThatThrownBy(() -> new SmtpMailSender(noHost, properties("from@example.com", "to@example.com")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("SMTP_HOST");
    }

    @Test
    void htmlMailArrivesAsTextPlusHtmlWithTheInlineImageAttached() throws Exception {
        var sender = new SmtpMailSender(smtpClient(), properties("from@example.com", "to@example.com"));
        byte[] png = java.util.Base64.getDecoder().decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==");
        var email = new EmailMessage("[시뮬레이션] 아침 판 데일리 브리핑", "글자판 본문 한글",
                "<html><body><p>HTML 본문 한글</p><img src=\"cid:chart-1\"></body></html>",
                java.util.List.of(new EmailMessage.InlineImage("chart-1", "image/png", png)));

        sender.send(email);

        MimeMessage[] received = GREEN_MAIL.getReceivedMessages();
        assertThat(received).hasSize(1);
        assertThat(received[0].getSubject()).isEqualTo(email.subject());
        // mixed > related > (alternative(text/plain, text/html), 인라인 image)
        assertThat(received[0].isMimeType("multipart/mixed")).isTrue();
        var mixed = (jakarta.mail.Multipart) received[0].getContent();
        assertThat(mixed.getCount()).isEqualTo(1);
        var related = mixed.getBodyPart(0);
        assertThat(related.isMimeType("multipart/related")).isTrue();
        var relatedParts = (jakarta.mail.Multipart) related.getContent();
        assertThat(relatedParts.getCount()).isEqualTo(2);
        var alternative = relatedParts.getBodyPart(0);
        assertThat(alternative.isMimeType("multipart/alternative")).isTrue();
        var alternativeParts = (jakarta.mail.Multipart) alternative.getContent();
        assertThat(alternativeParts.getCount()).isEqualTo(2);
        assertThat(alternativeParts.getBodyPart(0).isMimeType("text/plain")).isTrue();
        assertThat(alternativeParts.getBodyPart(1).isMimeType("text/html")).isTrue();
        assertThat(relatedParts.getBodyPart(1).isMimeType("image/png")).isTrue();

        var parts = new java.util.ArrayList<jakarta.mail.Part>();
        collectLeaves(received[0], parts);
        var plain = parts.stream().filter(p -> leafType(p, "text/plain")).findFirst().orElseThrow();
        var html = parts.stream().filter(p -> leafType(p, "text/html")).findFirst().orElseThrow();
        var image = parts.stream().filter(p -> leafType(p, "image/png")).findFirst().orElseThrow();
        assertThat(((String) plain.getContent()).strip()).isEqualTo("글자판 본문 한글");
        assertThat((String) html.getContent()).contains("HTML 본문 한글").contains("cid:chart-1");
        assertThat(image.getHeader("Content-ID")[0]).isEqualTo("<chart-1>");
        assertThat(image.getDisposition()).isEqualTo(jakarta.mail.Part.INLINE);
        assertThat(image.getInputStream().readAllBytes()).isEqualTo(png);
    }

    private static final String PNG_B64 =
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==";

    private static dev.stockreading.notifier.event.DailyBriefingEnvelope daily(
            java.util.List<dev.stockreading.notifier.event.DailyBriefingEnvelope.Image> images) {
        var payload = new dev.stockreading.notifier.event.DailyBriefingEnvelope.Payload(
                "morning", "2026-10-19T08:45:00+09:00", "2026-10-20T08:45:00+09:00", "글자판", java.util.List.of(),
                "llm", "<html><body><p>HTML</p></body></html>", images);
        return new dev.stockreading.notifier.event.DailyBriefingEnvelope(
                "5c2e9a14-7b3d-4f61-a8e0-3d9b6c1f2a40", "briefing.daily.generated", 1,
                "2026-10-20T08:45:00+09:00", "2026-10-20T08:45:41+09:00", "corr-1", payload);
    }

    private static java.util.List<String> sendDailyAndListInlineContentIds(
            java.util.List<dev.stockreading.notifier.event.DailyBriefingEnvelope.Image> images) throws Exception {
        var props = properties("from@example.com", "to@example.com");
        var email = new DailyEmailComposer(props).compose(daily(images));
        new SmtpMailSender(smtpClient(), props).send(email);

        MimeMessage[] received = GREEN_MAIL.getReceivedMessages();
        assertThat(received).hasSize(1);
        var parts = new java.util.ArrayList<jakarta.mail.Part>();
        collectLeaves(received[0], parts);
        // HTML 묶음 메일은 그대로 나간다: 글자판과 HTML이 한 alternative 안에 있다
        assertThat(parts.stream().filter(p -> leafType(p, "text/plain")).count()).isEqualTo(1);
        assertThat(parts.stream().filter(p -> leafType(p, "text/html")).count()).isEqualTo(1);
        assertThat(hasAlternative(received[0])).isTrue();
        return parts.stream().filter(p -> leafType(p, "image/*")).map(p -> {
            try {
                return p.getHeader("Content-ID")[0];
            } catch (jakarta.mail.MessagingException error) {
                throw new IllegalStateException(error);
            }
        }).toList();
    }

    private static boolean hasAlternative(jakarta.mail.Part part) throws Exception {
        if (part.isMimeType("multipart/alternative")) {
            return true;
        }
        if (part.isMimeType("multipart/*")) {
            var multipart = (jakarta.mail.Multipart) part.getContent();
            for (int i = 0; i < multipart.getCount(); i++) {
                if (hasAlternative(multipart.getBodyPart(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource(delimiter = '|', value = {
            "broken base64|bad1|image/png|!!not base64!!",
            "bad type|bad2|image/svg+xml|" + PNG_B64,
            "bad content id|bad 3|image/png|" + PNG_B64,
            "empty data|bad4|image/png|"})
    void aBadImageIsLeftOutButTheMultipartHtmlMailStillArrives(String name, String contentId, String type, String data)
            throws Exception {
        var bad = new dev.stockreading.notifier.event.DailyBriefingEnvelope.Image(contentId, type, data == null ? "" : data);
        var good = new dev.stockreading.notifier.event.DailyBriefingEnvelope.Image("good", "image/png", PNG_B64);

        assertThat(sendDailyAndListInlineContentIds(java.util.List.of(bad))).isEmpty();
        GREEN_MAIL.purgeEmailFromAllMailboxes();
        // 정상 그림과 섞이면 정상 그림은 남는다
        assertThat(sendDailyAndListInlineContentIds(java.util.List.of(bad, good))).containsExactly("<good>");
    }

    @Test
    void whenEveryImageIsBadTheMultipartHtmlMailStillArrivesWithoutImages() throws Exception {
        var bad1 = new dev.stockreading.notifier.event.DailyBriefingEnvelope.Image("a", "image/png", "!!");
        var bad2 = new dev.stockreading.notifier.event.DailyBriefingEnvelope.Image("b", "text/html", PNG_B64);

        assertThat(sendDailyAndListInlineContentIds(java.util.List.of(bad1, bad2))).isEmpty();
    }

    @Test
    void textOnlyMailIsStillASinglePartPlainMail() throws Exception {
        var sender = new SmtpMailSender(smtpClient(), properties("from@example.com", "to@example.com"));

        sender.send(new EmailMessage("subject", "plain body"));

        MimeMessage[] received = GREEN_MAIL.getReceivedMessages();
        assertThat(received[0].isMimeType("text/plain")).isTrue();
    }

    private static void collectLeaves(jakarta.mail.Part part, java.util.List<jakarta.mail.Part> out) throws Exception {
        if (part.isMimeType("multipart/*")) {
            var multipart = (jakarta.mail.Multipart) part.getContent();
            for (int i = 0; i < multipart.getCount(); i++) {
                collectLeaves(multipart.getBodyPart(i), out);
            }
        } else {
            out.add(part);
        }
    }

    private static boolean leafType(jakarta.mail.Part part, String type) {
        try {
            return part.isMimeType(type);
        } catch (jakarta.mail.MessagingException error) {
            throw new IllegalStateException(error);
        }
    }
}

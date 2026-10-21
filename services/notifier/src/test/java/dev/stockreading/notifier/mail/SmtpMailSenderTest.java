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
}

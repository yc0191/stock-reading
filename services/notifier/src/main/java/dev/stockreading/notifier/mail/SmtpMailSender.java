package dev.stockreading.notifier.mail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import dev.stockreading.notifier.NotifierProperties;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;

/** SMTP로 실제 발송한다(AWS SES SMTP 인터페이스 또는 다른 SMTP 서버). 발송이 켜져 있을 때만 활성화된다. */
@Component
@ConditionalOnProperty(name = "notifier.send-enabled", havingValue = "true")
class SmtpMailSender implements MailSenderPort {

    private static final Logger log = LoggerFactory.getLogger(SmtpMailSender.class);

    private final JavaMailSender mailSender;
    private final String sender;
    private final String recipient;

    SmtpMailSender(JavaMailSender mailSender, NotifierProperties properties) {
        if (isBlank(properties.sender()) || isBlank(properties.recipient())) {
            throw new IllegalStateException(
                    "notifier.sender and notifier.recipient must be set when notifier.send-enabled=true");
        }
        if (mailSender instanceof JavaMailSenderImpl impl && isBlank(impl.getHost())) {
            throw new IllegalStateException("spring.mail.host (SMTP_HOST) must be set when notifier.send-enabled=true");
        }
        this.mailSender = mailSender;
        this.sender = properties.sender();
        this.recipient = properties.recipient();
    }

    @Override
    public void send(EmailMessage message) {
        try {
            MimeMessage mime = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mime, false, "UTF-8");
            helper.setFrom(sender);
            helper.setTo(recipient);
            helper.setSubject(message.subject());
            helper.setText(message.body(), false);
            mailSender.send(mime);
            log.info("email sent to={} subject=\"{}\"", MailAddresses.mask(recipient), message.subject());
        } catch (MessagingException error) {
            throw new IllegalStateException("could not build email", error);
        } catch (MailException error) {
            // SMTP 서버의 메시지에 우리 주소나 AWS 계정 번호가 들어 있을 수 있다: 마스킹하고 원인은 연결하지 않는다.
            throw new MailDeliveryException(
                    "email delivery failed (" + error.getClass().getSimpleName() + "): " + MailAddresses.redact(error.getMessage()));
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}

package dev.stockreading.notifier.mail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import dev.stockreading.notifier.NotifierProperties;

/** 드라이런(기본): 보낼 내용을 보여 준다. 아무것도 밖으로 나가지 않는다. */
@Component
@ConditionalOnProperty(name = "notifier.send-enabled", havingValue = "false", matchIfMissing = true)
class LoggingMailSender implements MailSenderPort {

    private static final Logger log = LoggerFactory.getLogger(LoggingMailSender.class);

    private final NotifierProperties properties;

    LoggingMailSender(NotifierProperties properties) {
        this.properties = properties;
    }

    @Override
    public void send(EmailMessage message) {
        log.info("DRY-RUN (not sent) to={} subject=\"{}\"{}\n{}",
                MailAddresses.mask(properties.recipient()), message.subject(), htmlSummary(message), message.body());
    }

    /** HTML과 그림은 내용을 찍지 않고 크기만 남긴다(그림 base64가 로그를 채우지 않게). */
    private static String htmlSummary(EmailMessage message) {
        if (!message.hasHtml()) {
            return "";
        }
        String sizes = message.images().stream().map(image -> image.data().length + "B").toList().toString();
        return " html=" + message.html().length() + "chars images=" + message.images().size() + sizes;
    }
}

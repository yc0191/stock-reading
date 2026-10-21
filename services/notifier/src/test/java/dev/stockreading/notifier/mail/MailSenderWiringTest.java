package dev.stockreading.notifier.mail;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import dev.stockreading.notifier.NotifierProperties;

/** 안전 규칙: 명시적으로 켜지 않으면 발송기는 드라이런 로거뿐이다. */
class MailSenderWiringTest {

    @Configuration
    @EnableConfigurationProperties(NotifierProperties.class)
    @Import({LoggingMailSender.class, SmtpMailSender.class})
    static class Wiring {
        @Bean
        JavaMailSender javaMailSender() {
            JavaMailSenderImpl sender = new JavaMailSenderImpl();
            sender.setHost("localhost");
            return sender;
        }
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(Wiring.class);

    @Test
    void withNoSettingsTheDryRunLoggerIsUsedAndNothingCanBeSent() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(LoggingMailSender.class);
            assertThat(context).doesNotHaveBean(SmtpMailSender.class);
        });
    }

    @Test
    void explicitlyDisabledAlsoUsesTheDryRunLogger() {
        runner.withPropertyValues("notifier.send-enabled=false").run(context -> {
            assertThat(context).hasSingleBean(LoggingMailSender.class);
            assertThat(context).doesNotHaveBean(SmtpMailSender.class);
        });
    }

    @Test
    void enablingSendingSwitchesToSmtpOnly() {
        runner.withPropertyValues(
                        "notifier.send-enabled=true",
                        "notifier.sender=from@example.com",
                        "notifier.recipient=to@example.com")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(SmtpMailSender.class);
                    assertThat(context).doesNotHaveBean(LoggingMailSender.class);
                });
    }

    @Test
    void enablingSendingWithoutARecipientFailsFastInsteadOfSilentlyDoingNothing() {
        runner.withPropertyValues("notifier.send-enabled=true", "notifier.sender=from@example.com")
                .run(context -> assertThat(context).hasFailed());
    }
}

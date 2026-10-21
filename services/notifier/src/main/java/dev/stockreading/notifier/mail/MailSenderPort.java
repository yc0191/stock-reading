package dev.stockreading.notifier.mail;

public interface MailSenderPort {

    void send(EmailMessage message);
}

package dev.stockreading.notifier.mail;

public interface MailSenderPort {

    /** 받는 사람 한 명에게 메일 한 통을 보낸다. 받는 사람은 보내기 직전에 DB에서 고른 주소다. */
    void send(String recipient, EmailMessage message);
}

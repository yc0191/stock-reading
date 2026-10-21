package dev.stockreading.notifier.mail;

/**
 * 발송 실패. 메시지는 이미 마스킹되어 있고(주소·계정 번호 없음), 원본 예외는 일부러 연결하지 않는다.
 * 원인 체인을 로깅하면 마스킹되지 않은 원문이 출력되기 때문이다.
 */
public class MailDeliveryException extends RuntimeException {

    public MailDeliveryException(String redactedMessage) {
        super(redactedMessage);
    }
}

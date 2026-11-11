package dev.stockreading.notifier.recipient;

/**
 * 메일을 받을 사람 한 명. mailId는 member_mail 행의 번호로, 같은 이벤트를 같은 사람에게 두 번 보내지 않는 기준이다.
 * 주소는 개인정보라 toString에서 가린다.
 */
public record Recipient(long mailId, String address) {

    @Override
    public String toString() {
        return "Recipient[mailId=" + mailId + ", address=***]";
    }
}

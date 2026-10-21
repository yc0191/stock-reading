package dev.stockreading.notifier;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param sendEnabled   false = 드라이런: 이메일을 로그로만 남기고 아무것도 밖으로 나가지 않는다(안전한 기본값).
 * @param sender        발신 주소(SES로 보낼 때는 SES에서 인증된 주소여야 한다).
 * @param recipient     수신 주소. 개인정보이므로 git 제외 .env에만 두고 코드·테스트에는 넣지 않는다.
 * @param subjectPrefix 모든 제목 앞에 붙는 표식. 데이터가 가짜인 동안 쓴다. 예: "[시뮬레이션]".
 * @param maxPerHour    실제 발송 기준 슬라이딩 윈도우 한도.
 */
@ConfigurationProperties("notifier")
public record NotifierProperties(
        boolean sendEnabled,
        String sender,
        String recipient,
        String subjectPrefix,
        int maxPerHour) {
}

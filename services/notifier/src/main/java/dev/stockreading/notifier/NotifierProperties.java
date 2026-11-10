package dev.stockreading.notifier;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param sendEnabled   false = 드라이런: 이메일을 로그로만 남기고 아무것도 밖으로 나가지 않는다(안전한 기본값).
 * @param sender        발신 주소(SES로 보낼 때는 SES에서 인증된 주소여야 한다).
 * @param subjectPrefix 모든 제목 앞에 붙는 표식. 데이터가 가짜인 동안 쓴다. 예: "[시뮬레이션]".
 * @param maxPerHour    받는 사람 한 명당 시간당 급변동 메일 수(슬라이딩 윈도우). 데일리 판 메일은 세지 않는다.
 */
@ConfigurationProperties("notifier")
public record NotifierProperties(
        boolean sendEnabled,
        String sender,
        String subjectPrefix,
        int maxPerHour) {
}

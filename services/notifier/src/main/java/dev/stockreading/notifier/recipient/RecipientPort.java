package dev.stockreading.notifier.recipient;

import java.util.List;

/** 메일을 보내기 직전에 받는 사람을 고른다. 결과는 보내는 순간의 DB 값이라 재시작 없이 바뀐 설정이 반영된다. */
public interface RecipientPort {

    /** 급변동 메일: 사용 중이고 급변동 수신을 켠 멤버 중 그 종목을 포트폴리오(ETF 또는 주식)에 가진 멤버. */
    List<Recipient> forMove(String symbol);

    /** 데일리 판 메일: 사용 중이고 그 판 수신을 켠 멤버(판 종류는 morning 또는 post-close). 모르는 판 종류면 빈 목록이다. */
    List<Recipient> forEdition(String edition);
}

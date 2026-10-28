package dev.stockreading.collector.edgar;

import java.util.List;

/**
 * 미국 종목이 상장된 거래소. 해외 시세 조회가 거래소 코드를 고를 때 쓴다.
 * 값은 SEC 공시 이력(submissions)의 exchanges 를 그대로 돌려주며, 거래소 코드로 바꾸는 일은 쓰는 쪽이 한다.
 */
public interface ExchangePort {

    /** 티커의 상장 거래소 목록(SEC 응답 순서). 조회에 실패하면 {@link EdgarApiException} 을 던진다. */
    List<String> exchanges(String ticker);
}

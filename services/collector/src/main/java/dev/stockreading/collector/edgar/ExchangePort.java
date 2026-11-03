package dev.stockreading.collector.edgar;

import java.util.List;

/**
 * 미국 종목이 상장된 거래소. 해외 시세 조회가 거래소 코드를 고를 때 쓴다.
 * 값은 SEC 티커·거래소 매핑 파일의 거래소 값을 그대로 돌려주며, 거래소 코드로 바꾸는 일은 쓰는 쪽이 한다.
 */
public interface ExchangePort {

    /** 티커의 상장 거래소 목록. SEC 에 거래소가 없으면 빈 목록이고, 조회에 실패하면 {@link EdgarApiException} 을 던진다. */
    List<String> exchanges(String ticker);
}

package dev.stockreading.collector.edgar;

import java.util.List;

import org.springframework.stereotype.Component;

/** 상장 거래소를 SEC 티커·거래소 매핑 파일에서 읽는다. 파일은 클라이언트가 하루 한 번 받아 기억한다. */
@Component
class EdgarExchanges implements ExchangePort {

    private final EdgarPort edgar;

    EdgarExchanges(EdgarPort edgar) {
        this.edgar = edgar;
    }

    @Override
    public List<String> exchanges(String ticker) {
        return edgar.exchangesOf(ticker);
    }
}

package dev.stockreading.collector.holdings;

import java.util.List;

/**
 * 1초 현재가와 일봉을 받을 국내 종목코드. 값은 DB(personal.collect_target)에서 시작할 때 한 번 읽으며, 바꾸려면 재시작한다.
 * 구현은 값을 로그에 남기지 않는다.
 */
public interface QuoteSymbolsPort {

    List<String> symbols();
}

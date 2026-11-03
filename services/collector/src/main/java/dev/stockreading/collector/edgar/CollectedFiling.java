package dev.stockreading.collector.edgar;

import java.util.List;

/** 수집한 공시 한 건과, 그 회사(CIK)에 해당하는 보유 종목 티커들. 한 회사가 여러 종류 주식을 상장하면 티커가 여럿이다. */
public record CollectedFiling(String cik, List<String> tickers, Filing filing) {
}

package dev.stockreading.market.event;

import java.util.List;

/**
 * market.daily.responded의 payload. jobId와 seq는 요청 값을 그대로 되돌려준다.
 * 해외 등락률과 시장 배경은 market.context.responded와 같은 모양이라 그 타입을 그대로 쓴다.
 */
public record DailyPayload(
        String jobId,
        int seq,
        List<Etf> etfs,
        List<ContextPayload.CloseRate> closeRates,
        List<ContextPayload.Background> background,
        boolean truncated) {

    /** 보유 ETF 한 개의 하루 등락. date·close·prevClose·rate는 국내 일봉 원문 레코드 값을 공백만 걷어 옮긴다. */
    public record Etf(
            String symbol,
            String date,
            String close,
            String prevClose,
            String rate,
            String fetchedAt,
            List<String> tickers,
            boolean tickersTruncated) {
    }
}

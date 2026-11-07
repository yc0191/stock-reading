package dev.stockreading.market.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * market.daily.responded v2의 payload. jobId와 seq는 요청 값을 그대로 되돌려준다.
 * 해외 등락률과 시장 배경은 market.context.responded와 같은 모양이라 그 타입을 그대로 쓴다.
 */
public record DailyPayload(
        String jobId,
        int seq,
        List<Etf> etfs,
        List<ContextPayload.CloseRate> closeRates,
        List<ContextPayload.Background> background,
        boolean truncated) {

    /** 일봉이 없어 하루 등락 칸이 비었을 때의 이유. 값은 market.daily.responded v2 스키마의 enum과 같다. */
    public static final String NO_DAILY_PRICE_IN_RANGE = "no-daily-price-in-range";
    public static final String DAILY_PRICE_UNREADABLE = "daily-price-unreadable";

    /**
     * 관심 ETF 한 개의 하루 등락. date·close·prevClose·rate·fetchedAt은 국내 일봉 원문 레코드 값을 공백만 걷어 옮기며,
     * 일봉이 없거나 읽지 못하면 모두 null이고 missingReason에 이유가 있다.
     */
    public record Etf(
            String symbol,
            String date,
            String close,
            String prevClose,
            String rate,
            String fetchedAt,
            String missingReason,
            List<String> tickers,
            boolean tickersTruncated,
            @JsonInclude(JsonInclude.Include.NON_NULL) ContextPayload.EtfInfo etf) {

        public Etf(String symbol, String date, String close, String prevClose, String rate, String fetchedAt,
                List<String> tickers, boolean tickersTruncated, ContextPayload.EtfInfo etf) {
            this(symbol, date, close, prevClose, rate, fetchedAt, null, tickers, tickersTruncated, etf);
        }

        public Etf(String symbol, String date, String close, String prevClose, String rate, String fetchedAt,
                List<String> tickers, boolean tickersTruncated) {
            this(symbol, date, close, prevClose, rate, fetchedAt, null, tickers, tickersTruncated, null);
        }

        /** 일봉 없이 이유만 담은 ETF. */
        public static Etf withoutDaily(String symbol, String missingReason, List<String> tickers,
                boolean tickersTruncated, ContextPayload.EtfInfo etf) {
            return new Etf(symbol, null, null, null, null, null, missingReason, tickers, tickersTruncated, etf);
        }
    }
}

package dev.stockreading.market.event;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 내부 조회 API 의 응답 본문(옛 market.daily 응답 이벤트 v2의 payload 와 같은 모양). jobId와 seq는 요청 값을 그대로 되돌려준다.
 * 해외 등락률과 시장 배경은 시장 배경 응답(ContextPayload)과 같은 모양이라 그 타입을 그대로 쓴다.
 */
public record DailyPayload(
        String jobId,
        int seq,
        List<Etf> etfs,
        List<ContextPayload.CloseRate> closeRates,
        List<ContextPayload.Background> background,
        boolean truncated,
        @JsonInclude(JsonInclude.Include.NON_NULL) Operations operations) {

    public DailyPayload(String jobId, int seq, List<Etf> etfs, List<ContextPayload.CloseRate> closeRates,
            List<ContextPayload.Background> background, boolean truncated) {
        this(jobId, seq, etfs, closeRates, background, truncated, null);
    }

    /** 일봉이 없어 하루 등락 칸이 비었을 때의 이유. 값은 market.daily 응답 v2 스키마의 enum과 같다. */
    public static final String NO_DAILY_PRICE_IN_RANGE = "no-daily-price-in-range";
    public static final String DAILY_PRICE_UNREADABLE = "daily-price-unreadable";

    /**
     * 관심 ETF 한 개의 하루 등락. date·close·prevClose·rate·fetchedAt은 국내 일봉 원문 레코드 값을 공백만 걷어 옮기며,
     * 일봉이 없거나 읽지 못하면 모두 null이고 missingReason에 이유가 있다. 그때 판 시각 전에 받은 장중 시세가 있으면
     * prevClose·rate만 그 시세로 계산한 대체값으로 채우고 intradayFallback에 그 시세의 가격과 받은 시각을 싣는다.
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
            @JsonInclude(JsonInclude.Include.NON_NULL) ContextPayload.EtfInfo etf,
            @JsonInclude(JsonInclude.Include.NON_NULL) OpenFlow openFlow,
            @JsonInclude(JsonInclude.Include.NON_NULL) IntradayFallback intradayFallback,
            @JsonInclude(JsonInclude.Include.NON_NULL) IntradaySeries intradaySeries,
            @JsonInclude(JsonInclude.Include.NON_NULL) DailyCloses dailyCloses) {

        public Etf(String symbol, String date, String close, String prevClose, String rate, String fetchedAt,
                String missingReason, List<String> tickers, boolean tickersTruncated, ContextPayload.EtfInfo etf) {
            this(symbol, date, close, prevClose, rate, fetchedAt, missingReason, tickers, tickersTruncated, etf, null,
                    null, null, null);
        }

        public Etf withOpenFlow(OpenFlow flow) {
            return new Etf(symbol, date, close, prevClose, rate, fetchedAt, missingReason, tickers, tickersTruncated,
                    etf, flow, intradayFallback, intradaySeries, dailyCloses);
        }

        /** 일봉 없는 ETF에 장중 시세로 계산한 전일 종가·하루 등락을 채운다. missingReason은 일봉이 없는 이유로 남는다. */
        public Etf withIntradayFallback(String fallbackPrevClose, String fallbackRate, IntradayFallback fallback) {
            return new Etf(symbol, date, close, fallbackPrevClose, fallbackRate, fetchedAt, missingReason, tickers,
                    tickersTruncated, etf, openFlow, fallback, intradaySeries, dailyCloses);
        }

        public Etf withIntradaySeries(IntradaySeries series) {
            return new Etf(symbol, date, close, prevClose, rate, fetchedAt, missingReason, tickers, tickersTruncated,
                    etf, openFlow, intradayFallback, series, dailyCloses);
        }

        public Etf withDailyCloses(DailyCloses closes) {
            return new Etf(symbol, date, close, prevClose, rate, fetchedAt, missingReason, tickers, tickersTruncated,
                    etf, openFlow, intradayFallback, intradaySeries, closes);
        }

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

    /** 하루 등락 대체값에 쓴 장중 시세: 가격(원)과 시세처리기가 그 시세를 받은 시각. */
    public record IntradayFallback(String price, String receivedAt) {
    }

    /** 개장 흐름의 시가 출처: 시세 원문의 시가 칸 / 09:00 이후 첫 스냅샷 가격. */
    public static final String OPEN_SOURCE_RAW = "raw";
    public static final String OPEN_SOURCE_FIRST_SNAPSHOT = "first-snapshot";
    public static final String OPEN_FLOW_PRESENT = "present";
    public static final String OPEN_FLOW_NO_SNAPSHOT = "no-snapshot";

    /**
     * 아침 판용 개장 흐름. 요청이 openFlowSince를 보냈을 때만 ETF마다 담긴다. 값은 저장한 시세에서 코드가 계산한다.
     * 범위 안(요청 구간 시작 이상, 기준 시각 이하) 시세가 없으면 status만 no-snapshot이고 나머지는 null이다.
     * 비율은 %, 소수 둘째 자리 반올림 문자열이다. open이 시세 원문에서 읽히지 않으면 첫 스냅샷 가격이며 openSource에 표시한다.
     */
    public record OpenFlow(
            String status,
            String prevClose,
            String open,
            String openSource,
            String openRate,
            String startAt,
            String startPrice,
            String endAt,
            String endPrice,
            String flowRate,
            String rateVsPrevClose) {

        public static OpenFlow none() {
            return new OpenFlow(OPEN_FLOW_NO_SNAPSHOT, null, null, null, null, null, null, null, null, null, null);
        }
    }

    public static final String INTRADAY_SERIES_PRESENT = "present";
    public static final String INTRADAY_SERIES_NO_SNAPSHOT = "no-snapshot";

    /**
     * 그래프용 장중 흐름. 요청이 intradaySince를 보냈을 때만 ETF마다 담긴다. 범위 안 시세를 1분마다 그 분의 마지막 시세 한 점으로
     * 솎은 것이며 points는 시세 시각 오름차순이다. 범위 안 시세가 없으면 status만 no-snapshot이고 prevClose는 null, points는 비어 있다.
     * 점 수가 상한을 넘으면 늦은 점부터 남기고 truncated가 true다.
     */
    public record IntradaySeries(String status, String prevClose, boolean truncated, List<IntradayPoint> points) {

        public static IntradaySeries none() {
            return new IntradaySeries(INTRADAY_SERIES_NO_SNAPSHOT, null, false, List.of());
        }
    }

    /** 장중 흐름의 한 점: 시세 시각, 가격(원), 전일 종가 대비 등락률(%, 소수 둘째 자리 반올림, 전일 종가를 모르면 null). */
    public record IntradayPoint(String at, String price, String rateVsPrevClose) {
    }

    public static final String DAILY_CLOSES_PRESENT = "present";
    public static final String DAILY_CLOSES_NO_DAILY_PRICE = "no-daily-price";

    /**
     * 그래프용 기간 종가. 요청이 maxDailyCloses를 보냈을 때만 ETF마다 담긴다. 저장한 국내 일봉 원문들의 레코드를 일자로 모아
     * 같은 일자는 가장 늦게 받은 원문의 값을 쓰며, points는 일자 오름차순이다. fetchedAt은 쓴 원문 중 가장 늦게 받은 것의 받은 시각이다.
     * 원문이 없으면 status만 no-daily-price이고 fetchedAt은 null, points는 비어 있다. 거래일 수가 상한을 넘으면 최근 일자부터 남기고
     * truncated가 true다.
     */
    public record DailyCloses(String status, String fetchedAt, boolean truncated, List<DailyClose> points) {

        public static DailyCloses none() {
            return new DailyCloses(DAILY_CLOSES_NO_DAILY_PRICE, null, false, List.of());
        }
    }

    /** 기간 종가의 하루: 일자(yyyyMMdd), 종가, 등락률. 값은 일봉 원문을 공백만 걷어 옮기며 등락률이 없으면 null이다. */
    public record DailyClose(String date, String close, String rate) {
    }

    /**
     * 시세처리기 운영 상태. 요청이 includeOperations를 보냈을 때만 담긴다. 종목코드는 담지 않는다.
     * quoteFailures의 키는 market.daily 응답 v2 스키마가 정한 종류 이름이다.
     */
    public record Operations(
            List<CollectionStatus> collections,
            Map<String, Integer> quoteFailures,
            List<DroppedTopic> droppedMessages) {
    }

    /** 수집 종류 하나의 범위 안 수신 현황. 받은 자료가 없으면 두 시각은 null이고 receivedCount는 0이다. */
    public record CollectionStatus(String kind, String firstReceivedAt, String lastReceivedAt, long receivedCount) {
    }

    public record DroppedTopic(String topic, long count) {
    }
}

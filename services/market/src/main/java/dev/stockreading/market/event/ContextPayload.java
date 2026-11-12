package dev.stockreading.market.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/** market.context.responded의 payload. jobId와 seq는 요청 값을 그대로 되돌려준다. */
public record ContextPayload(
        String jobId,
        int seq,
        String symbol,
        List<String> tickers,
        List<CloseRate> closeRates,
        List<Background> background,
        boolean truncated,
        @JsonInclude(JsonInclude.Include.NON_NULL) EtfInfo etf,
        @JsonInclude(JsonInclude.Include.NON_NULL) String name) {

    public ContextPayload(String jobId, int seq, String symbol, List<String> tickers, List<CloseRate> closeRates,
            List<Background> background, boolean truncated) {
        this(jobId, seq, symbol, tickers, closeRates, background, truncated, null, null);
    }

    /**
     * ETF 정보(선택). 변동 종목이 보유 종목 파일에 ETF로 있을 때만 담긴다. 비중(weight)은 %를 문자열로 옮기고,
     * 모르는 칸은 빠진다. 구성 종목이 maxTickers보다 많으면 비중 순위 순으로 잘라 membersTruncated를 true로 한다.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record EtfInfo(String name, String tracks, String sector, List<EtfMember> members,
            boolean membersTruncated, MemberChanges memberChanges) {

        public EtfInfo(String name, String tracks, String sector, List<EtfMember> members, boolean membersTruncated) {
            this(name, tracks, sector, members, membersTruncated, null);
        }

        /** 상위 구성 변경을 더한 사본. 데일리 응답(market.daily.responded v2)에만 쓴다. */
        public EtfInfo withMemberChanges(MemberChanges changes) {
            return new EtfInfo(name, tracks, sector, members, membersTruncated, changes);
        }
    }

    /**
     * ETF 상위 구성 종목의 변경(선택). since·asOf는 yyyy-MM-dd이고, 종목은 removed·added·moved 중 한 곳에만 나온다.
     * 비중은 담지 않는다.
     */
    public record MemberChanges(String since, String asOf, List<ChangedMember> removed, List<ChangedMember> added,
            List<MovedMember> moved) {
    }

    /** 상위 구성에서 빠졌거나 들어온 종목. rank는 빠지기 전 또는 들어온 뒤의 비중 순위다. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ChangedMember(String market, String symbol, String name, int rank) {
    }

    /** 상위 구성 안에서 순위만 바뀐 종목. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record MovedMember(String market, String symbol, String name, int rankBefore, int rankAfter) {
    }

    /** ETF 구성 종목 한 개. rank는 비중 순위(1이 가장 큼)다. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record EtfMember(String market, String symbol, String name, String weight, int rank) {
    }

    /** 미국 종목 한 개의 직전 정규장 등락률. date와 rate는 저장된 원문 그대로다. */
    public record CloseRate(String ticker, String date, String closeAt, String rate, String fetchedAt) {
    }

    /**
     * 배경 자료 한 건. status에 따라 담는 필드가 다르고(ok: response·fetchedAt, too-large: fetchedAt·responseBytes,
     * missing: 없음), 빈 필드는 직렬화에서 빠진다. target은 값이 없어도 null로 실어야 해서 NON_NULL에서 뺀다.
     * indexRates는 세계지수에만 있는 선택 필드로, 원문의 등락률 대신 market이 계산한 지수별 등락률이다.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Background(
            String apiType,
            @JsonInclude(JsonInclude.Include.ALWAYS) String target,
            String status,
            String response,
            String fetchedAt,
            Integer responseBytes,
            List<IndexRate> indexRates) {

        public Background(String apiType, String target, String status, String response, String fetchedAt,
                Integer responseBytes) {
            this(apiType, target, status, response, fetchedAt, responseBytes, null);
        }

        public static Background ok(String apiType, String target, String response, String fetchedAt) {
            return new Background(apiType, target, "ok", response, fetchedAt, null);
        }

        public static Background tooLarge(String apiType, String target, String fetchedAt, int responseBytes) {
            return new Background(apiType, target, "too-large", null, fetchedAt, responseBytes);
        }

        public static Background missing(String apiType, String target) {
            return new Background(apiType, target, "missing", null, null, null);
        }

        public Background withIndexRates(List<IndexRate> rates) {
            return new Background(apiType, target, status, response, fetchedAt, responseBytes, List.copyOf(rates));
        }
    }

    /**
     * 세계지수 한 지수의 계산된 등락률. 값은 원문을 공백만 걷어 옮긴다. 직전 행을 찾지 못하면 prev*와 rate가
     * null이고, 이 경우에도 키는 실어야 "없음"이 드러나므로 NON_NULL을 쓰지 않는다.
     */
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public record IndexRate(
            String code,
            String name,
            String businessDay,
            String close,
            String prevBusinessDay,
            String prevClose,
            String rate) {
    }
}

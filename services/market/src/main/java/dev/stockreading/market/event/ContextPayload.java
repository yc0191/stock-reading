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
        boolean truncated) {

    /** 미국 종목 한 개의 직전 정규장 등락률. date와 rate는 저장된 원문 그대로다. */
    public record CloseRate(String ticker, String date, String closeAt, String rate, String fetchedAt) {
    }

    /**
     * 배경 자료 한 건. status에 따라 담는 필드가 다르고(ok: response·fetchedAt, too-large: fetchedAt·responseBytes,
     * missing: 없음), 빈 필드는 직렬화에서 빠진다. target은 값이 없어도 null로 실어야 해서 NON_NULL에서 뺀다.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Background(
            String apiType,
            @JsonInclude(JsonInclude.Include.ALWAYS) String target,
            String status,
            String response,
            String fetchedAt,
            Integer responseBytes) {

        public static Background ok(String apiType, String target, String response, String fetchedAt) {
            return new Background(apiType, target, "ok", response, fetchedAt, null);
        }

        public static Background tooLarge(String apiType, String target, String fetchedAt, int responseBytes) {
            return new Background(apiType, target, "too-large", null, fetchedAt, responseBytes);
        }

        public static Background missing(String apiType, String target) {
            return new Background(apiType, target, "missing", null, null, null);
        }
    }
}

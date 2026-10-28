package dev.stockreading.collector.marketdata;

/**
 * 종목 하나의 조회 실패 기록. 키, 시크릿, 접근토큰 값은 담지 않는다.
 * 선택 값은 알 수 없으면 null이다.
 */
public record QuoteFailure(
        String symbol,
        FailureType type,
        Integer httpStatus,
        String responseCode,
        Long elapsedMs,
        String exceptionType,
        Long tokenAgeMs,
        Long tokenRemainingMs,
        Integer concurrentCalls,
        String responseRaw) {

    /** 호출 없이 건너뛴 종목. exceptionType 으로 건너뜀의 사유를 구분한다(예: RateLimited). */
    public static QuoteFailure skipped(String symbol, String exceptionType) {
        return new QuoteFailure(symbol, FailureType.TICK_SKIPPED, null, null, null, exceptionType, null, null, null, null);
    }
}

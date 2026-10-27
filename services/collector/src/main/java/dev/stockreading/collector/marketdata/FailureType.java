package dev.stockreading.collector.marketdata;

/** market.snapshot.failed 계약의 failureType 값과 이름이 같아야 한다. */
public enum FailureType {
    TOKEN_ISSUE_FAILED,
    HTTP_ERROR,
    RESULT_CODE_FAILED,
    EMPTY_PRICE,
    PRICE_PARSE_FAILED,
    CONNECTION_FAILED,
    TIMEOUT,
    TICK_SKIPPED
}

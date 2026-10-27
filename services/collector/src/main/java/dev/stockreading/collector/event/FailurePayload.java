package dev.stockreading.collector.event;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * market.snapshot.failed의 payload. 키, 시크릿, 접근토큰 값과 인증 헤더는 담지 않는다.
 * 선택 필드는 null이면 직렬화에서 빠진다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FailurePayload(
        String symbol,
        String failureType,
        String scheduledFor,
        Integer httpStatus,
        String responseCode,
        Long elapsedMs,
        String exceptionType,
        TokenState tokenState,
        Integer concurrentCalls,
        Integer tickSucceeded,
        Integer tickFailed,
        String responseRaw) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TokenState(Long ageMs, Long remainingMs) {
    }
}

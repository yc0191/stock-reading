package dev.stockreading.news.api;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.UUID;

/**
 * 내부 조회 API 입구의 요청 칸 검사. 칸이 빠지거나 형식이 틀리면 {@link InvalidRequestException} 을 던진다. 예외 메시지에는
 * jobId 와 칸 이름만 담는다. 칸 값은 보유 종목이 드러날 수 있어 담지 않는다.
 */
public final class RequestChecks {

    private final String jobId;

    private RequestChecks(String jobId) {
        this.jobId = jobId;
    }

    /** jobId 는 응답 짝짓기에 쓰는 필수 칸이라 먼저 확인한다. */
    public static RequestChecks of(String jobId) {
        RequestChecks checks = new RequestChecks(jobId);
        checks.text("jobId", jobId);
        return checks;
    }

    public String text(String field, String value) {
        if (value == null || value.isBlank()) {
            throw invalid(field);
        }
        return value;
    }

    public OffsetDateTime time(String field, String value) {
        text(field, value);
        try {
            return OffsetDateTime.parse(value);
        } catch (DateTimeException e) {
            throw invalid(field);
        }
    }

    /** 숫자 칸은 빠지면 0 으로 읽히므로 1 이상인지로 누락까지 함께 걸러 낸다. */
    public void positive(String field, int value) {
        if (value < 1) {
            throw invalid(field);
        }
    }

    public void uuid(String field, String value) {
        text(field, value);
        try {
            UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw invalid(field);
        }
    }

    public void notAfter(String field, OffsetDateTime value, OffsetDateTime limit) {
        if (value.isAfter(limit)) {
            throw invalid(field);
        }
    }

    public void notEmpty(String field, Collection<String> values) {
        if (values == null || values.isEmpty() || values.stream().anyMatch(v -> v == null || v.isBlank())) {
            throw invalid(field);
        }
    }

    private InvalidRequestException invalid(String field) {
        return new InvalidRequestException("jobId=" + jobId + " field=" + field);
    }
}

package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class QuoteFailureRepository {

    private final JdbcClient jdbc;

    QuoteFailureRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 멱등 삽입: 다시 전달된 이벤트(같은 eventId)는 무시한다. */
    boolean insertIfAbsent(UUID eventId, String correlationId, OffsetDateTime occurredAt,
            FailureEnvelope.FailurePayload payload, OffsetDateTime scheduledFor, String responseRawJson) {
        FailureEnvelope.TokenState token = payload.tokenState();
        int rows = jdbc.sql("""
                        INSERT INTO quote_failure (event_id, correlation_id, symbol, failure_type, occurred_at,
                            scheduled_for, http_status, response_code, elapsed_ms, exception_type,
                            token_age_ms, token_remaining_ms, concurrent_calls, tick_succeeded, tick_failed,
                            response_raw)
                        VALUES (:eventId, :correlationId, :symbol, :failureType, :occurredAt,
                            :scheduledFor, :httpStatus, :responseCode, :elapsedMs, :exceptionType,
                            :tokenAgeMs, :tokenRemainingMs, :concurrentCalls, :tickSucceeded, :tickFailed,
                            CAST(CAST(:responseRaw AS text) AS jsonb))
                        ON CONFLICT (event_id) DO NOTHING
                        """)
                .param("eventId", eventId)
                .param("correlationId", correlationId)
                .param("symbol", payload.symbol())
                .param("failureType", payload.failureType())
                .param("occurredAt", occurredAt)
                .param("scheduledFor", scheduledFor)
                .param("httpStatus", payload.httpStatus())
                .param("responseCode", payload.responseCode())
                .param("elapsedMs", payload.elapsedMs())
                .param("exceptionType", payload.exceptionType())
                .param("tokenAgeMs", token == null ? null : token.ageMs())
                .param("tokenRemainingMs", token == null ? null : token.remainingMs())
                .param("concurrentCalls", payload.concurrentCalls())
                .param("tickSucceeded", payload.tickSucceeded())
                .param("tickFailed", payload.tickFailed())
                .param("responseRaw", responseRawJson)
                .update();
        return rows == 1;
    }
}

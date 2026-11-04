package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.json.JsonMapper;

class QuoteFailureListenerTest {

    private static final UUID EVENT_ID = UUID.fromString("3e9d1c52-8a47-4f60-b2d1-6c7a9e0f4b25");

    private QuoteFailureRepository repository;
    private QuoteFailureListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(QuoteFailureRepository.class);
        listener = new QuoteFailureListener(repository, JsonMapper.builder().build());
    }

    @Test
    void storesEveryFieldOfTheFailureEvent() throws IOException {
        when(repository.insertIfAbsent(any(), any(), any(), any(), any(), any())).thenReturn(true);

        listener.onFailure(ContractSchemas.sample("market.snapshot.failed.sample.json"));

        ArgumentCaptor<FailureEnvelope.FailurePayload> payload = ArgumentCaptor.forClass(FailureEnvelope.FailurePayload.class);
        ArgumentCaptor<String> raw = ArgumentCaptor.forClass(String.class);
        verify(repository).insertIfAbsent(eq(EVENT_ID), eq("collect-2026-10-20T16:00:00+09:00"),
                eq(OffsetDateTime.parse("2026-10-20T16:00:02+09:00")), payload.capture(),
                eq(OffsetDateTime.parse("2026-10-20T16:00:00+09:00")), raw.capture());
        assertThat(payload.getValue().symbol()).isEqualTo("TEST01");
        assertThat(payload.getValue().failureType()).isEqualTo("HTTP_ERROR");
        assertThat(payload.getValue().httpStatus()).isEqualTo(500);
        assertThat(payload.getValue().tokenState().remainingMs()).isEqualTo(82_800_000L);
        assertThat(payload.getValue().tickFailed()).isEqualTo(1);
        assertThat(raw.getValue()).isEqualTo("{\"example\":\"fake-error-response\"}");
    }

    @Test
    void sameFailureEventDeliveredTwiceIsIgnoredTheSecondTime() throws IOException {
        when(repository.insertIfAbsent(any(), any(), any(), any(), any(), any())).thenReturn(true, false);
        String message = ContractSchemas.sample("market.snapshot.failed.sample.json");

        listener.onFailure(message);
        listener.onFailure(message);

        // 중복 판정은 DB의 event_id 유니크가 하며, 두 번째 호출에서도 예외 없이 끝나야 한다.
        verify(repository, times(2)).insertIfAbsent(eq(EVENT_ID), any(), any(), any(), any(), any());
    }

    @Test
    void nonJsonResponseBodyIsKeptAsAJsonString() throws IOException {
        when(repository.insertIfAbsent(any(), any(), any(), any(), any(), any())).thenReturn(true);
        String message = ContractSchemas.sample("market.snapshot.failed.sample.json")
                .replace("\"responseRaw\": \"{\\\"example\\\":\\\"fake-error-response\\\"}\"", "\"responseRaw\": \"  <html>bad gateway</html>\\n\"");
        assertThat(message).contains("<html>");

        listener.onFailure(message);

        ArgumentCaptor<String> raw = ArgumentCaptor.forClass(String.class);
        verify(repository).insertIfAbsent(any(), any(), any(), any(), any(), raw.capture());
        assertThat(raw.getValue()).isEqualTo("\"<html>bad gateway</html>\"");
    }

    @Test
    void fieldMismatchFailureIsStoredWithItsChecksAndRawResponse() throws IOException {
        when(repository.insertIfAbsent(any(), any(), any(), any(), any(), any())).thenReturn(true);
        String message = ContractSchemas.sample("market.snapshot.failed.sample.json")
                .replace("\"HTTP_ERROR\"", "\"FIELD_MISMATCH\"")
                .replace("\"HttpStatusException\"", "\"FieldMismatch:CHANGE+RATE\"");
        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", message)).isEmpty();

        listener.onFailure(message);

        ArgumentCaptor<FailureEnvelope.FailurePayload> payload = ArgumentCaptor.forClass(FailureEnvelope.FailurePayload.class);
        ArgumentCaptor<String> raw = ArgumentCaptor.forClass(String.class);
        verify(repository).insertIfAbsent(eq(EVENT_ID), any(), any(), payload.capture(), any(), raw.capture());
        assertThat(payload.getValue().failureType()).isEqualTo("FIELD_MISMATCH");
        assertThat(payload.getValue().exceptionType()).isEqualTo("FieldMismatch:CHANGE+RATE");
        assertThat(raw.getValue()).isEqualTo("{\"example\":\"fake-error-response\"}");
    }

    @Test
    void failureWithoutOptionalFieldsIsStored() throws IOException {
        when(repository.insertIfAbsent(any(), any(), any(), any(), any(), any())).thenReturn(true);
        String minimal = """
                {"eventId":"3e9d1c52-8a47-4f60-b2d1-6c7a9e0f4b25","eventType":"market.snapshot.failed",
                 "schemaVersion":1,"occurredAt":"2026-10-20T16:00:02+09:00","producedAt":"2026-10-20T16:00:02+09:00",
                 "correlationId":"c","payload":{"symbol":"TEST01","failureType":"TICK_SKIPPED",
                 "scheduledFor":"2026-10-20T16:00:00+09:00","unknownFutureField":1}}
                """;
        assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", minimal)).isEmpty();

        listener.onFailure(minimal);

        verify(repository).insertIfAbsent(eq(EVENT_ID), eq("c"), any(), any(), any(), isNull());
    }
}

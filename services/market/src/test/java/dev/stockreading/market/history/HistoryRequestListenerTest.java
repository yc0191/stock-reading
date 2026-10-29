package dev.stockreading.market.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import dev.stockreading.market.event.HistoryPayload;
import tools.jackson.databind.json.JsonMapper;

class HistoryRequestListenerTest {

    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-20T15:55:31+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T16:10:31+09:00");

    private PriceHistoryRepository repository;
    private HistoryPublisher publisher;
    private HistoryRequestListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(PriceHistoryRepository.class);
        publisher = mock(HistoryPublisher.class);
        listener = new HistoryRequestListener(repository, publisher, JsonMapper.builder().build());
    }

    private static List<PriceHistoryRepository.StoredPoint> rows(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new PriceHistoryRepository.StoredPoint(SINCE.plusSeconds(i), 10_000L + i))
                .toList();
    }

    @Test
    void respondsWithEveryRowInTheRequestedRangeAndEchoesTheIdentifiers() throws IOException {
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), anyInt())).thenReturn(rows(3));

        listener.onRequest(ContractSchemas.sample("market.history.requested.sample.json"));

        ArgumentCaptor<List<HistoryPayload.Point>> points = ArgumentCaptor.captor();
        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), points.capture(), eq(false), eq(CORRELATION_ID));
        assertThat(points.getValue()).hasSize(3);
        assertThat(points.getValue().get(0)).isEqualTo(new HistoryPayload.Point("2026-10-20T15:55:31+09:00", 10_000L));
    }

    @Test
    void writesUtcRowsAsKstWithSeconds() throws IOException {
        OffsetDateTime utc = OffsetDateTime.parse("2026-10-20T06:55:00Z");
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), anyInt()))
                .thenReturn(List.of(new PriceHistoryRepository.StoredPoint(utc, 10_000L)));

        listener.onRequest(ContractSchemas.sample("market.history.requested.sample.json"));

        ArgumentCaptor<List<HistoryPayload.Point>> points = ArgumentCaptor.captor();
        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), points.capture(), eq(false), eq(CORRELATION_ID));
        assertThat(points.getValue().get(0).asOf()).isEqualTo("2026-10-20T15:55:00+09:00");
    }

    @Test
    void zeroRowsIsStillAnswered() throws IOException {
        when(repository.findBetweenInclusive(anyString(), any(), any(), anyInt())).thenReturn(List.of());

        listener.onRequest(ContractSchemas.sample("market.history.requested.sample.json"));

        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), eq(List.of()), eq(false), eq(CORRELATION_ID));
    }

    @Test
    void readsOneRowMoreThanTheLimitAndMarksTheResponseTruncated() throws IOException {
        String request = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 2");
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), eq(3))).thenReturn(rows(3));

        listener.onRequest(request);

        ArgumentCaptor<List<HistoryPayload.Point>> points = ArgumentCaptor.captor();
        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), points.capture(), eq(true), eq(CORRELATION_ID));
        // 뒤쪽을 버려 구간 앞쪽(기준 가격)이 남는다.
        assertThat(points.getValue()).extracting(HistoryPayload.Point::price).containsExactly(10_000L, 10_001L);
    }

    @Test
    void exactlyTheLimitIsNotTruncated() throws IOException {
        String request = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 3");
        when(repository.findBetweenInclusive(anyString(), any(), any(), eq(4))).thenReturn(rows(3));

        listener.onRequest(request);

        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), any(), eq(false), eq(CORRELATION_ID));
    }

    @Test
    void capsTheRequestedLimitSoTheResponseStaysUnderTheMessageSizeLimit() throws IOException {
        String request = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 1000000");
        when(repository.findBetweenInclusive(anyString(), any(), any(), anyInt())).thenReturn(List.of());

        listener.onRequest(request);

        verify(repository).findBetweenInclusive("TEST01", SINCE, UNTIL, HistoryRequestListener.MAX_POINTS_CAP + 1);
    }

    @Test
    void invalidRequestIsRejectedWithoutQueryingOrAnswering() throws IOException {
        String zeroLimit = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 0");
        String reversedRange = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"since\": \"2026-10-20T15:55:31+09:00\"", "\"since\": \"2026-10-20T17:00:00+09:00\"");

        assertThatThrownBy(() -> listener.onRequest(zeroLimit)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> listener.onRequest(reversedRange)).isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(repository, publisher);
    }

    @Test
    void doesNotAnswerWhenTheQueryFails() throws IOException {
        when(repository.findBetweenInclusive(anyString(), any(), any(), anyInt()))
                .thenThrow(new IllegalStateException("db down"));

        assertThatThrownBy(() -> listener.onRequest(ContractSchemas.sample("market.history.requested.sample.json")))
                .isInstanceOf(IllegalStateException.class);

        verify(publisher, org.mockito.Mockito.never()).publish(anyString(), anyInt(), anyString(), any(), anyBoolean(), anyString());
    }
}

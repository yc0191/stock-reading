package dev.stockreading.news.filing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.json.JsonMapper;

class FilingListenerTest {

    private static final UUID EVENT_ID = UUID.fromString("9c1e2d3f-4a5b-4c6d-8e7f-a0b1c2d3e4f5");

    private FilingRepository repository;
    private FilingListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(FilingRepository.class);
        listener = new FilingListener(repository, JsonMapper.builder().build());
    }

    @Test
    void mapsTheEventToAFiling() {
        when(repository.save(any())).thenReturn(true);

        listener.onFiling(event(EVENT_ID, "0000000001-26-000001", "[\"TESTA\"]", "8-K", "[\"2.02\",\"9.01\"]"));

        assertThat(savedFilings(1).get(0)).isEqualTo(new Filing(
                EVENT_ID,
                "0000000001-26-000001",
                "0000000001",
                "8-K",
                List.of("2.02", "9.01"),
                OffsetDateTime.parse("2026-10-20T20:30:28Z"),
                LocalDate.parse("2026-10-20"),
                "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/test-8k.htm",
                List.of("TESTA")));
    }

    @Test
    void sameEventDeliveredTwiceIsSavedWithTheSameFilingBothTimes() {
        // 멱등성은 저장소가 eventId와 접수번호 유니크로 보장하므로 소비자는 같은 내용으로 같은 저장 호출만 해야 한다.
        when(repository.save(any())).thenReturn(true, false);
        String message = event(EVENT_ID, "0000000001-26-000001", "[\"TESTA\"]", "8-K", "[\"2.02\"]");

        listener.onFiling(message);
        listener.onFiling(message);

        List<Filing> saved = savedFilings(2);
        assertThat(saved.get(0)).isEqualTo(saved.get(1));
    }

    @Test
    void formWithoutItemNumbersKeepsAnEmptyItemList() {
        when(repository.save(any())).thenReturn(true);

        listener.onFiling(event(EVENT_ID, "0000000002-26-000002", "[\"TESTB\",\"TESTB.A\"]", "6-K", "[]"));

        Filing filing = savedFilings(1).get(0);
        assertThat(filing.form()).isEqualTo("6-K");
        assertThat(filing.items()).isEmpty();
        assertThat(filing.tickers()).containsExactly("TESTB", "TESTB.A");
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() {
        when(repository.save(any())).thenReturn(true);
        String message = event(EVENT_ID, "0000000001-26-000001", "[\"TESTA\"]", "8-K", "[]")
                .replace("\"payload\":", "\"futureEnvelopeField\":1,\"payload\":")
                .replace("\"cik\":", "\"futurePayloadField\":\"x\",\"cik\":");

        listener.onFiling(message);

        verify(repository).save(any());
    }

    @Test
    void unreadableEventFailsInsteadOfBeingDropped() {
        // 예외가 나가야 공용 오류 처리기가 재시도 후 failed_event에 남긴다.
        assertThatThrownBy(() -> listener.onFiling("{bad")).isNotNull();
        assertThatThrownBy(() -> listener.onFiling(
                event(EVENT_ID, "0000000001-26-000001", "[\"TESTA\"]", "8-K", "[]")
                        .replace("2026-10-20T20:30:28Z\",\"filingDate", "not-a-time\",\"filingDate")))
                .isNotNull();

        verify(repository, never()).save(any());
    }

    private List<Filing> savedFilings(int expectedCalls) {
        ArgumentCaptor<Filing> captor = ArgumentCaptor.forClass(Filing.class);
        verify(repository, times(expectedCalls)).save(captor.capture());
        return captor.getAllValues();
    }

    private static String event(UUID eventId, String accession, String tickersJson, String form, String itemsJson) {
        return """
                {"eventId":"%s","eventType":"filing.collected","schemaVersion":1,
                 "occurredAt":"2026-10-20T20:30:28Z","producedAt":"2026-10-21T05:45:01.120+09:00",
                 "correlationId":"collect-filings-2026-10-21T05:45:00+09:00",
                 "payload":{"tickers":%s,"cik":"0000000001","accessionNumber":"%s","form":"%s","items":%s,
                            "acceptedAt":"2026-10-20T20:30:28Z","filingDate":"2026-10-20",
                            "url":"https://www.sec.gov/Archives/edgar/data/1/000000000126000001/test-8k.htm"}}
                """.formatted(eventId, tickersJson, accession, form, itemsJson);
    }
}

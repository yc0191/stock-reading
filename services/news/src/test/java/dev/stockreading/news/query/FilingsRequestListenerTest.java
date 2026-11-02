package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.news.event.FilingsPayload;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 저장소를 대역으로 바꿔 공시 요청 한 건의 응답 전체를 돌린다. 값은 모두 가짜다. */
class FilingsRequestListenerTest {

    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String REQUEST_EVENT_ID = "c6f02a9d-8e3b-4d17-a5c4-9b1e7f3d2a68";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-17T16:00:31+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T16:00:31+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private FilingQueryRepository repository;
    private FilingsPublisher publisher;
    private FilingsRequestListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(FilingQueryRepository.class);
        publisher = mock(FilingsPublisher.class);
        listener = new FilingsRequestListener(repository, publisher, mapper);
    }

    private static String request() throws IOException {
        return ContractSchemas.sample("news.filings.requested.sample.json");
    }

    private static List<FilingQueryRepository.StoredFiling> rows(int count) {
        // 최신순: 첫 행이 가장 늦게 접수됐다.
        return IntStream.range(0, count)
                .mapToObj(i -> new FilingQueryRepository.StoredFiling(
                        String.format("0000000000-26-%06d", 100 - i), "8-K", List.of("2.02"),
                        UNTIL.minusHours(i).withOffsetSameInstant(java.time.ZoneOffset.UTC),
                        "https://www.example.com/filings/" + i, List.of("TESTA")))
                .toList();
    }

    @SuppressWarnings("unchecked")
    private List<FilingsPayload.Filing> published(boolean truncated) {
        ArgumentCaptor<List<FilingsPayload.Filing>> filings = ArgumentCaptor.captor();
        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), filings.capture(), eq(truncated),
                eq(REQUEST_EVENT_ID), eq(CORRELATION_ID));
        return filings.getValue();
    }

    @Test
    void answersWithTheRequestedRangeEndsIncludedAndEchoesTheIdentifiers() throws IOException {
        when(repository.findByTickers(List.of("TESTA", "FAKEB"), SINCE, UNTIL, 21)).thenReturn(rows(2));

        listener.onRequest(request());

        List<FilingsPayload.Filing> filings = published(false);
        assertThat(filings).extracting(FilingsPayload.Filing::accessionNumber)
                .containsExactly("0000000000-26-000100", "0000000000-26-000099");
        // 저장소의 UTC 시각을 KST로 바꿔 싣는다.
        assertThat(filings.get(0).acceptedAt()).isEqualTo("2026-10-20T16:00:31+09:00");
    }

    @Test
    void filingsOverTheLimitKeepTheNewestAndAreMarkedTruncated() throws IOException {
        String request = request().replace("\"maxFilings\": 20", "\"maxFilings\": 2");
        when(repository.findByTickers(anyList(), any(), any(), eq(3))).thenReturn(rows(3));

        listener.onRequest(request);

        assertThat(published(true)).extracting(FilingsPayload.Filing::accessionNumber)
                .containsExactly("0000000000-26-000100", "0000000000-26-000099");
    }

    @Test
    void exactlyTheLimitIsNotTruncated() throws IOException {
        String request = request().replace("\"maxFilings\": 20", "\"maxFilings\": 2");
        when(repository.findByTickers(anyList(), any(), any(), eq(3))).thenReturn(rows(2));

        listener.onRequest(request);

        assertThat(published(false)).hasSize(2);
    }

    @Test
    void capsTheRequestedLimit() throws IOException {
        String request = request().replace("\"maxFilings\": 20", "\"maxFilings\": 1000000");
        when(repository.findByTickers(anyList(), any(), any(), anyInt())).thenReturn(List.of());

        listener.onRequest(request);

        verify(repository).findByTickers(List.of("TESTA", "FAKEB"), SINCE, UNTIL,
                FilingsRequestListener.MAX_FILINGS_CAP + 1);
        assertThat(published(false)).isEmpty();
    }

    @Test
    void classShareWrittenWithADotIsLookedUpWithTheStoredDash() throws IOException {
        String request = request().replace("[\"TESTA\", \"FAKEB\"]", "[\"TEST.A\", \"TEST-A\"]");
        when(repository.findByTickers(anyList(), any(), any(), anyInt())).thenReturn(List.of());

        listener.onRequest(request);

        verify(repository).findByTickers(List.of("TEST-A"), SINCE, UNTIL, 21);
    }

    @Test
    void invalidRequestIsRejectedWithoutQueryingOrAnswering() throws IOException {
        String zeroLimit = request().replace("\"maxFilings\": 20", "\"maxFilings\": 0");
        String reversed = request().replace("\"since\": \"2026-10-17T16:00:31+09:00\"",
                "\"since\": \"2026-10-20T16:00:32+09:00\"");
        String noTickers = request().replace("[\"TESTA\", \"FAKEB\"]", "[]");

        for (String invalid : List.of(zeroLimit, reversed, noTickers)) {
            assertThatThrownBy(() -> listener.onRequest(invalid)).isInstanceOf(IllegalArgumentException.class);
        }
        verifyNoInteractions(repository, publisher);
    }

    @Test
    void sameRequestDeliveredTwiceGetsTheSameAnswer() throws IOException {
        when(repository.findByTickers(anyList(), any(), any(), anyInt())).thenReturn(rows(1));

        listener.onRequest(request());
        listener.onRequest(request());

        ArgumentCaptor<List<FilingsPayload.Filing>> filings = ArgumentCaptor.captor();
        verify(publisher, org.mockito.Mockito.times(2)).publish(eq(JOB_ID), eq(1), eq("TEST01"), filings.capture(),
                eq(false), eq(REQUEST_EVENT_ID), eq(CORRELATION_ID));
        assertThat(filings.getAllValues().get(0)).isEqualTo(filings.getAllValues().get(1));
    }

    @SuppressWarnings("unchecked")
    @Test
    void publishedResponseMatchesTheContract() throws IOException {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        FilingsPublisher real = new FilingsPublisher(kafka, mapper, CLOCK);
        FilingsPayload.Filing filing = new FilingsPayload.Filing("0000000000-26-000100", List.of("TESTA"), "8-K",
                List.of("2.02"), "2026-10-20T16:00:31+09:00", "https://www.example.com/filings/1");

        real.publish(JOB_ID, 3, "TEST01", List.of(filing), true, REQUEST_EVENT_ID, CORRELATION_ID);

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("news.filings.responded"), eq("TEST01"), json.capture());
        assertThat(ContractSchemas.validate("news.filings.responded.v1.schema.json", json.getValue())).isEmpty();
        assertThat(mapper.readTree(json.getValue()).at("/payload/seq").asInt()).isEqualTo(3);
        assertThat(mapper.readTree(json.getValue()).at("/payload/truncated").asBoolean()).isTrue();
    }
}

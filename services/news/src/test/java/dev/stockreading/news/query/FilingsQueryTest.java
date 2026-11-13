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

import dev.stockreading.news.event.FilingsPayload;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 저장소를 대역으로 바꿔 공시 요청 한 건의 응답 전체를 돌린다. 값은 모두 가짜다. */
class FilingsQueryTest {

    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String REQUEST_EVENT_ID = "c6f02a9d-8e3b-4d17-a5c4-9b1e7f3d2a68";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-17T16:00:31+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T16:00:31+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private FilingQueryRepository repository;
    private FilingsQuery listener;
    private FilingsPayload last;

    @BeforeEach
    void setUp() {
        repository = mock(FilingQueryRepository.class);
        listener = new FilingsQuery(repository);
    }

    // 요청 샘플(옛 요청 이벤트)의 payload 가 내부 조회 API 의 요청 본문이다.
    private void answer(String request) {
        last = listener.answer(mapper.treeToValue(mapper.readTree(request).get("payload"), FilingsRequest.class));
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

    /** 마지막 답이 요청의 식별자를 되돌려주고 잘림 표시가 기대대로인지 보고 공시 목록을 돌려준다. */
    private List<FilingsPayload.Filing> published(boolean truncated) {
        assertThat(last.jobId()).isEqualTo(JOB_ID);
        assertThat(last.seq()).isEqualTo(1);
        assertThat(last.symbol()).isEqualTo("TEST01");
        assertThat(last.truncated()).isEqualTo(truncated);
        return last.filings();
    }

    @Test
    void answersWithTheRequestedRangeEndsIncludedAndEchoesTheIdentifiers() throws IOException {
        when(repository.findByTickers(List.of("TESTA", "FAKEB"), SINCE, UNTIL, 21)).thenReturn(rows(2));

        answer(request());

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

        answer(request);

        assertThat(published(true)).extracting(FilingsPayload.Filing::accessionNumber)
                .containsExactly("0000000000-26-000100", "0000000000-26-000099");
    }

    @Test
    void exactlyTheLimitIsNotTruncated() throws IOException {
        String request = request().replace("\"maxFilings\": 20", "\"maxFilings\": 2");
        when(repository.findByTickers(anyList(), any(), any(), eq(3))).thenReturn(rows(2));

        answer(request);

        assertThat(published(false)).hasSize(2);
    }

    @Test
    void capsTheRequestedLimit() throws IOException {
        String request = request().replace("\"maxFilings\": 20", "\"maxFilings\": 1000000");
        when(repository.findByTickers(anyList(), any(), any(), anyInt())).thenReturn(List.of());

        answer(request);

        verify(repository).findByTickers(List.of("TESTA", "FAKEB"), SINCE, UNTIL,
                FilingsQuery.MAX_FILINGS_CAP + 1);
        assertThat(published(false)).isEmpty();
    }

    @Test
    void classShareWrittenWithADotIsLookedUpWithTheStoredDash() throws IOException {
        String request = request().replace("[\"TESTA\", \"FAKEB\"]", "[\"TEST.A\", \"TEST-A\"]");
        when(repository.findByTickers(anyList(), any(), any(), anyInt())).thenReturn(List.of());

        answer(request);

        verify(repository).findByTickers(List.of("TEST-A"), SINCE, UNTIL, 21);
    }

    @Test
    void invalidRequestIsRejectedWithoutQueryingOrAnswering() throws IOException {
        String zeroLimit = request().replace("\"maxFilings\": 20", "\"maxFilings\": 0");
        String reversed = request().replace("\"since\": \"2026-10-17T16:00:31+09:00\"",
                "\"since\": \"2026-10-20T16:00:32+09:00\"");
        String noTickers = request().replace("[\"TESTA\", \"FAKEB\"]", "[]");

        for (String invalid : List.of(zeroLimit, reversed, noTickers)) {
            assertThatThrownBy(() -> answer(invalid)).isInstanceOf(IllegalArgumentException.class);
        }
        verifyNoInteractions(repository);
    }

    @Test
    void sameRequestDeliveredTwiceGetsTheSameAnswer() throws IOException {
        when(repository.findByTickers(anyList(), any(), any(), anyInt())).thenReturn(rows(1));

        answer(request());
        FilingsPayload first = last;
        answer(request());

        assertThat(last).isEqualTo(first);
    }
}

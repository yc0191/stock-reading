package dev.stockreading.market.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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

import dev.stockreading.market.event.HistoryPayload;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class HistoryQueryTest {

    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-20T15:55:31+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T16:10:31+09:00");

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private PriceHistoryRepository repository;
    private HistoryQuery listener;
    private HistoryPayload last;

    @BeforeEach
    void setUp() {
        repository = mock(PriceHistoryRepository.class);
        listener = new HistoryQuery(repository);
    }

    // 요청 샘플(옛 요청 이벤트)의 payload 가 내부 조회 API 의 요청 본문이다.
    private void answer(String request) {
        last = listener.answer(mapper.treeToValue(mapper.readTree(request).get("payload"), HistoryRequest.class));
    }

    /** 마지막 답이 요청의 식별자를 되돌려주고 잘림 표시가 기대대로인지 보고 점 목록을 돌려준다. */
    private List<HistoryPayload.Point> published(boolean truncated) {
        assertThat(last.jobId()).isEqualTo(JOB_ID);
        assertThat(last.seq()).isEqualTo(1);
        assertThat(last.symbol()).isEqualTo("TEST01");
        assertThat(last.truncated()).isEqualTo(truncated);
        return last.points();
    }

    private static List<PriceHistoryRepository.StoredPoint> rows(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new PriceHistoryRepository.StoredPoint(SINCE.plusSeconds(i), 10_000L + i))
                .toList();
    }

    @Test
    void respondsWithEveryRowInTheRequestedRangeAndEchoesTheIdentifiers() throws IOException {
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), anyInt())).thenReturn(rows(3));

        answer(ContractSchemas.sample("market.history.requested.sample.json"));

        List<HistoryPayload.Point> points = published(false);
        assertThat(points).hasSize(3);
        assertThat(points.get(0)).isEqualTo(new HistoryPayload.Point("2026-10-20T15:55:31+09:00", 10_000L));
    }

    @Test
    void writesUtcRowsAsKstWithSeconds() throws IOException {
        OffsetDateTime utc = OffsetDateTime.parse("2026-10-20T06:55:00Z");
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), anyInt()))
                .thenReturn(List.of(new PriceHistoryRepository.StoredPoint(utc, 10_000L)));

        answer(ContractSchemas.sample("market.history.requested.sample.json"));

        List<HistoryPayload.Point> points = published(false);
        assertThat(points.get(0).asOf()).isEqualTo("2026-10-20T15:55:00+09:00");
    }

    @Test
    void zeroRowsIsStillAnswered() throws IOException {
        when(repository.findBetweenInclusive(anyString(), any(), any(), anyInt())).thenReturn(List.of());

        answer(ContractSchemas.sample("market.history.requested.sample.json"));

        assertThat(published(false)).isEmpty();
    }

    @Test
    void readsOneRowMoreThanTheLimitAndMarksTheResponseTruncated() throws IOException {
        String request = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 2");
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), eq(3))).thenReturn(rows(3));

        answer(request);

        List<HistoryPayload.Point> points = published(true);
        // 뒤쪽을 버려 구간 앞쪽(기준 가격)이 남는다.
        assertThat(points).extracting(HistoryPayload.Point::price).containsExactly(10_000L, 10_001L);
    }

    @Test
    void exactlyTheLimitIsNotTruncated() throws IOException {
        String request = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 3");
        when(repository.findBetweenInclusive(anyString(), any(), any(), eq(4))).thenReturn(rows(3));

        answer(request);

        published(false);
    }

    @Test
    void capsTheRequestedLimitSoTheResponseStaysUnderTheMessageSizeLimit() throws IOException {
        String request = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 1000000");
        when(repository.findBetweenInclusive(anyString(), any(), any(), anyInt())).thenReturn(List.of());

        answer(request);

        verify(repository).findBetweenInclusive("TEST01", SINCE, UNTIL, HistoryQuery.MAX_POINTS_CAP + 1);
    }

    @Test
    void invalidRequestIsRejectedWithoutQueryingOrAnswering() throws IOException {
        String zeroLimit = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 0");
        String reversedRange = ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"since\": \"2026-10-20T15:55:31+09:00\"", "\"since\": \"2026-10-20T17:00:00+09:00\"");

        assertThatThrownBy(() -> answer(zeroLimit)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> answer(reversedRange)).isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(repository);
    }

    private static String withInterval(int maxPoints, int intervalSeconds) throws IOException {
        return ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": " + maxPoints + ",\n    \"intervalSeconds\": " + intervalSeconds);
    }

    @Test
    void requestWithAnIntervalReadsThinnedRowsUpToTheLimit() throws IOException {
        when(repository.findThinnedBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), eq(10), eq(61)))
                .thenReturn(rows(60));

        answer(withInterval(60, 10));

        List<HistoryPayload.Point> points = published(false);
        assertThat(points).hasSize(60);
        verify(repository, org.mockito.Mockito.never()).findBetweenInclusive(anyString(), any(), any(), anyInt());
    }

    @Test
    void thinnedRowsOverTheLimitAreCutFromTheEndAndMarkedTruncated() throws IOException {
        when(repository.findThinnedBetweenInclusive(anyString(), any(), any(), eq(10), eq(3))).thenReturn(rows(3));

        answer(withInterval(2, 10));

        List<HistoryPayload.Point> points = published(true);
        assertThat(points).extracting(HistoryPayload.Point::price).containsExactly(10_000L, 10_001L);
    }

    @Test
    void olderRequestWithoutAnIntervalIsNotThinned() throws IOException {
        when(repository.findBetweenInclusive(anyString(), any(), any(), anyInt())).thenReturn(rows(2));

        answer(ContractSchemas.sample("market.history.requested.sample.json"));

        verify(repository).findBetweenInclusive("TEST01", SINCE, UNTIL, 5001);
        verify(repository, org.mockito.Mockito.never())
                .findThinnedBetweenInclusive(anyString(), any(), any(), anyInt(), anyInt());
    }

    @Test
    void zeroIntervalIsRejectedWithoutQueryingOrAnswering() throws IOException {
        assertThatThrownBy(() -> answer(withInterval(60, 0))).isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(repository);
    }

    @Test
    void doesNotAnswerWhenTheQueryFails() throws IOException {
        when(repository.findBetweenInclusive(anyString(), any(), any(), anyInt()))
                .thenThrow(new IllegalStateException("db down"));

        assertThatThrownBy(() -> answer(ContractSchemas.sample("market.history.requested.sample.json")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(last).isNull();
    }
}

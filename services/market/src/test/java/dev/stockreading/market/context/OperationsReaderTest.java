package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.stockreading.market.event.DailyPayload;

/** 저장소를 대역으로 바꿔 운영 상태를 계약 모양으로 조립하는 로직만 본다. 값은 모두 가짜다. */
class OperationsReaderTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-19T17:00:00+09:00");
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-20T17:00:00+09:00");

    private final OperationsRepository repository = mock(OperationsRepository.class);
    private final OperationsReader reader = new OperationsReader(repository);

    private DailyPayload.Operations read() {
        return reader.read(SINCE, AS_OF);
    }

    @Test
    void nothingReceivedStillGivesAllFourKindsWithZeroCountsAndEveryFailureKindAsZero() {
        when(repository.collections(any(), any())).thenReturn(List.of());
        when(repository.quoteFailures(any(), any())).thenReturn(Map.of());
        when(repository.droppedMessages(any(), any())).thenReturn(List.of());

        DailyPayload.Operations operations = read();

        assertThat(operations.collections()).extracting(DailyPayload.CollectionStatus::kind)
                .containsExactly("pre-open", "quote-1s", "intraday", "post-close");
        assertThat(operations.collections()).allSatisfy(c -> {
            assertThat(c.firstReceivedAt()).isNull();
            assertThat(c.lastReceivedAt()).isNull();
            assertThat(c.receivedCount()).isZero();
        });
        assertThat(operations.quoteFailures()).containsOnlyKeys(OperationsReader.QUOTE_FAILURE_KINDS)
                .containsValues(0);
        assertThat(operations.droppedMessages()).isEmpty();
    }

    @Test
    void receivedKindsKeepTheirTimesInKstAndMissingKindsStayEmpty() {
        when(repository.collections(any(), any())).thenReturn(List.of(
                new OperationsRepository.CollectionRow("quote-1s", OffsetDateTime.parse("2026-10-20T00:00:01Z"),
                        OffsetDateTime.parse("2026-10-20T06:30:00Z"), 70_200)));
        when(repository.quoteFailures(any(), any())).thenReturn(Map.of());
        when(repository.droppedMessages(any(), any())).thenReturn(List.of());

        DailyPayload.Operations operations = read();

        assertThat(operations.collections().get(1)).isEqualTo(new DailyPayload.CollectionStatus("quote-1s",
                "2026-10-20T09:00:01+09:00", "2026-10-20T15:30:00+09:00", 70_200));
        assertThat(operations.collections().get(0).receivedCount()).isZero();
        assertThat(operations.collections().get(3).lastReceivedAt()).isNull();
    }

    @Test
    void failureCountsOverwriteTheZerosAndAnUnknownKindIsKeptByName() {
        when(repository.collections(any(), any())).thenReturn(List.of());
        when(repository.quoteFailures(any(), any()))
                .thenReturn(Map.of("EMPTY_PRICE", 3, "TICK_SKIPPED_RATE_LIMITED", 5, "SOMETHING_NEW", 1));
        when(repository.droppedMessages(any(), any())).thenReturn(List.of());

        Map<String, Integer> failures = read().quoteFailures();

        assertThat(failures).containsEntry("EMPTY_PRICE", 3).containsEntry("TICK_SKIPPED_RATE_LIMITED", 5)
                .containsEntry("SOMETHING_NEW", 1).containsEntry("TIMEOUT", 0);
    }

    @Test
    void droppedMessagesCarryTheirTopicAndCount() {
        when(repository.collections(any(), any())).thenReturn(List.of());
        when(repository.quoteFailures(any(), any())).thenReturn(Map.of());
        when(repository.droppedMessages(any(), any())).thenReturn(List.of(
                new OperationsRepository.DroppedRow("market.insight", 2),
                new OperationsRepository.DroppedRow("market.snapshot", 1)));

        assertThat(read().droppedMessages()).containsExactly(new DailyPayload.DroppedTopic("market.insight", 2),
                new DailyPayload.DroppedTopic("market.snapshot", 1));
    }
}

package dev.stockreading.market.snapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

class SnapshotListenerTest {

    private static final UUID EVENT_ID = UUID.fromString("7a1f9e60-2b3c-4d5e-8f90-a1b2c3d4e5f6");
    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-10-20T16:00:01+09:00");
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:00+09:00";

    private PriceSnapshotRepository repository;
    private MovePublisher movePublisher;
    private SnapshotListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(PriceSnapshotRepository.class);
        movePublisher = mock(MovePublisher.class);
        listener = new SnapshotListener(repository, movePublisher, JsonMapper.builder().build());
    }

    @Test
    void storesTheSnapshotAndLooksUpThePreviousPrice() throws IOException {
        when(repository.insertIfAbsent(eq(EVENT_ID), eq("TEST01"), any(OffsetDateTime.class), eq(10_250L))).thenReturn(true);
        when(repository.findPreviousPrice(eq("TEST01"), any(OffsetDateTime.class))).thenReturn(Optional.of(10_000L));

        listener.onSnapshot(ContractSchemas.sample("market.snapshot.sample.json"));

        verify(repository).insertIfAbsent(EVENT_ID, "TEST01", AS_OF, 10_250L);
        verify(repository).findPreviousPrice("TEST01", AS_OF);
    }

    @Test
    void forwardsTheComputedChangeAndTheCorrelationIdToTheMovePublisher() throws IOException {
        when(repository.insertIfAbsent(any(UUID.class), anyString(), any(OffsetDateTime.class), anyLong())).thenReturn(true);
        when(repository.findPreviousPrice(anyString(), any(OffsetDateTime.class))).thenReturn(Optional.of(10_000L));

        listener.onSnapshot(ContractSchemas.sample("market.snapshot.sample.json"));

        // 10000 -> 10250은 +2.50%이며, 상관관계 ID가 파이프라인을 따라 이어져야 한다.
        verify(movePublisher).publishIfSignificant(
                "TEST01", 10_000L, 10_250L, new BigDecimal("2.50"), AS_OF, CORRELATION_ID);
    }

    @Test
    void redeliveredEventIsIgnoredWithoutFurtherWork() throws IOException {
        when(repository.insertIfAbsent(any(UUID.class), anyString(), any(OffsetDateTime.class), anyLong())).thenReturn(false);

        listener.onSnapshot(ContractSchemas.sample("market.snapshot.sample.json"));

        verify(repository, never()).findPreviousPrice(anyString(), any(OffsetDateTime.class));
        verifyNoInteractions(movePublisher);
    }

    @Test
    void firstSnapshotOfAnInstrumentHasNoChangeToPublish() throws IOException {
        when(repository.insertIfAbsent(any(UUID.class), anyString(), any(OffsetDateTime.class), anyLong())).thenReturn(true);
        when(repository.findPreviousPrice(anyString(), any(OffsetDateTime.class))).thenReturn(Optional.empty());

        listener.onSnapshot(ContractSchemas.sample("market.snapshot.sample.json"));

        verify(repository).findPreviousPrice("TEST01", AS_OF);
        verifyNoInteractions(movePublisher);
    }
}

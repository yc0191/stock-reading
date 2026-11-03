package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.OffsetDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PricePartitionsTest {

    private PartitionPort port;
    private PricePartitions partitions;

    @BeforeEach
    void setUp() {
        port = mock(PartitionPort.class);
        partitions = new PricePartitions(port);
    }

    @Test
    void createsThePartitionOfAnUnseenDayWithKstMidnightBounds() {
        partitions.ensure(OffsetDateTime.parse("2026-10-20T16:00:01+09:00"));

        verify(port).createIfAbsent("price_snapshot_p20261020",
                OffsetDateTime.parse("2026-10-20T00:00:00+09:00"),
                OffsetDateTime.parse("2026-10-21T00:00:00+09:00"));
    }

    @Test
    void usesTheKstDayEvenWhenAsOfIsGivenInAnotherOffset() {
        // 2026-10-20T15:30Z는 한국 시간으로 10-21 00:30이다.
        partitions.ensure(OffsetDateTime.parse("2026-10-20T15:30:00Z"));

        verify(port).createIfAbsent(org.mockito.ArgumentMatchers.eq("price_snapshot_p20261021"), any(), any());
    }

    @Test
    void kstMidnightBelongsToTheNextDay() {
        partitions.ensure(OffsetDateTime.parse("2026-10-20T23:59:59+09:00"));
        partitions.ensure(OffsetDateTime.parse("2026-10-21T00:00:00+09:00"));

        verify(port).createIfAbsent(org.mockito.ArgumentMatchers.eq("price_snapshot_p20261020"), any(), any());
        verify(port).createIfAbsent(org.mockito.ArgumentMatchers.eq("price_snapshot_p20261021"), any(), any());
    }

    @Test
    void asksTheDatabaseOnlyOncePerDay() {
        partitions.ensure(OffsetDateTime.parse("2026-10-20T10:00:00+09:00"));
        partitions.ensure(OffsetDateTime.parse("2026-10-20T10:00:01+09:00"));
        partitions.ensure(OffsetDateTime.parse("2026-10-20T23:00:00+09:00"));

        verify(port, times(1)).createIfAbsent(anyString(), any(), any());
    }

    @Test
    void retriesOnTheNextEventWhenCreationFailed() {
        doThrow(new IllegalStateException("ddl failed")).doNothing().when(port).createIfAbsent(anyString(), any(), any());
        OffsetDateTime asOf = OffsetDateTime.parse("2026-10-20T10:00:00+09:00");

        assertThatThrownBy(() -> partitions.ensure(asOf)).isInstanceOf(IllegalStateException.class);
        partitions.ensure(asOf);

        verify(port, times(2)).createIfAbsent(anyString(), any(), any());
        partitions.ensure(asOf);
        verify(port, times(2)).createIfAbsent(anyString(), any(), any());
    }
}

package dev.stockreading.market.holdings;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.stockreading.market.event.HoldingsPayload.Instrument;

/** 가짜 저장소와 가짜 발행기로 "바뀔 때만 발행"을 확인한다. */
class HoldingsChangeWatcherTest {

    private static final HoldingsSnapshot ONE = new HoldingsSnapshot(
            List.of(new Instrument("ETF", "KR", "TEST01", "HELD", null)), List.of());
    private static final HoldingsSnapshot TWO = new HoldingsSnapshot(
            List.of(new Instrument("ETF", "KR", "TEST01", "HELD", null),
                    new Instrument("ETF", "KR", "TEST02", "WATCH", null)), List.of());

    private HoldingsRepository repository;
    private HoldingsPublisher publisher;
    private HoldingsChangeWatcher watcher;

    @BeforeEach
    void setUp() {
        repository = mock(HoldingsRepository.class);
        publisher = mock(HoldingsPublisher.class);
        watcher = new HoldingsChangeWatcher(repository, publisher);
    }

    @Test
    void firstCheckAfterStartPublishesOnceEvenIfNothingChanged() throws Exception {
        when(repository.readSnapshot()).thenReturn(ONE);

        watcher.check();

        verify(publisher).publish(ONE);
    }

    @Test
    void sameListIsNotPublishedAgainButAChangeIs() throws Exception {
        when(repository.readSnapshot()).thenReturn(ONE, ONE, ONE, TWO, TWO);

        for (int i = 0; i < 5; i++) {
            watcher.check();
        }

        verify(publisher, times(1)).publish(ONE);
        verify(publisher, times(1)).publish(TWO);
    }

    @Test
    void aFailedPublishIsRetriedOnTheNextCheck() throws Exception {
        when(repository.readSnapshot()).thenReturn(ONE);
        doThrow(new IllegalStateException("broker down")).when(publisher).publish(any());

        watcher.check();
        doNothing().when(publisher).publish(any());
        watcher.check();
        watcher.check();

        verify(publisher, times(2)).publish(ONE);
    }

    @Test
    void aFailedReadDoesNotPublishAndDoesNotStopLaterChecks() throws Exception {
        when(repository.readSnapshot()).thenThrow(new IllegalStateException("db down")).thenReturn(ONE);

        watcher.check();
        verify(publisher, never()).publish(any());
        watcher.check();

        verify(publisher).publish(ONE);
    }
}

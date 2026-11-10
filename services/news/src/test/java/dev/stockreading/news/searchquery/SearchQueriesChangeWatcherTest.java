package dev.stockreading.news.searchquery;

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

import dev.stockreading.news.event.SearchQueriesPayload.Query;

/** 가짜 저장소와 가짜 발행기로 "바뀔 때만 발행"을 확인한다. */
class SearchQueriesChangeWatcherTest {

    private static final SearchQuerySnapshot ONE = new SearchQuerySnapshot(List.of(new Query("가짜", List.of("TEST01"))));
    private static final SearchQuerySnapshot TWO = new SearchQuerySnapshot(
            List.of(new Query("가짜", List.of("TEST01")), new Query("가짜 둘", List.of("*"))));

    private SearchQueryRepository repository;
    private SearchQueriesPublisher publisher;
    private SearchQueriesChangeWatcher watcher;

    @BeforeEach
    void setUp() {
        repository = mock(SearchQueryRepository.class);
        publisher = mock(SearchQueriesPublisher.class);
        watcher = new SearchQueriesChangeWatcher(repository, publisher);
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

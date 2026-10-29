package dev.stockreading.collector.edgar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class EdgarCallSpacingTest {

    private static final long SECOND = 1_000_000_000L;

    private long now = 5 * SECOND;
    private final List<Long> sleeps = new ArrayList<>();

    private EdgarCallSpacing spacing(int maxCalls) {
        return new EdgarCallSpacing(maxCalls, () -> now, nanos -> {
            sleeps.add(nanos);
            now += nanos;
        });
    }

    @Test
    void noOneSecondWindowHoldsMoreThanTheLimit() throws InterruptedException {
        EdgarCallSpacing spacing = spacing(10);
        List<Long> callTimes = new ArrayList<>();

        for (int i = 0; i < 35; i++) {
            spacing.await();
            callTimes.add(now);
        }

        for (long start : callTimes) {
            long inWindow = callTimes.stream().filter(t -> t >= start && t < start + SECOND).count();
            assertThat(inWindow).isLessThanOrEqualTo(10);
        }
        // 35번째 호출은 첫 호출 뒤 3.4초에 일어난다. 필요 이상으로 늦추지 않는다.
        assertThat(callTimes.get(34) - callTimes.get(0)).isEqualTo(34 * SECOND / 10);
    }

    @Test
    void doesNotWaitWhenCallsAreAlreadySpacedOut() throws InterruptedException {
        EdgarCallSpacing spacing = spacing(10);

        spacing.await();
        now += SECOND;
        spacing.await();

        assertThat(sleeps).isEmpty();
    }

    @Test
    void intervalIsRoundedUpSoTheLimitIsNeverExceeded() throws InterruptedException {
        EdgarCallSpacing spacing = spacing(3);

        spacing.await();
        spacing.await();

        assertThat(sleeps).containsExactly(333_333_334L);
    }
}

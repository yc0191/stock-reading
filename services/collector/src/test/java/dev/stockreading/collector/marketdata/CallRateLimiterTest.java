package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class CallRateLimiterTest {

    private final AtomicLong nanos = new AtomicLong();
    private final CallRateLimiter limiter = new CallRateLimiter(2, Duration.ofSeconds(1), nanos::get);

    @Test
    void rejectsCallsOverTheLimitWithinTheWindow() {
        assertThat(limiter.tryAcquire(Duration.ZERO)).isTrue();
        assertThat(limiter.tryAcquire(Duration.ZERO)).isTrue();
        assertThat(limiter.tryAcquire(Duration.ZERO)).isFalse();
    }

    @Test
    void allowsAgainAfterTheWindowPasses() {
        limiter.tryAcquire(Duration.ZERO);
        limiter.tryAcquire(Duration.ZERO);

        nanos.addAndGet(Duration.ofSeconds(1).toNanos());

        assertThat(limiter.tryAcquire(Duration.ZERO)).isTrue();
    }

    @Test
    void waitsForASlotWhenItFreesWithinTheGrace() {
        var real = new CallRateLimiter(1, Duration.ofMillis(100), System::nanoTime);
        assertThat(real.tryAcquire(Duration.ZERO)).isTrue();

        assertThat(real.tryAcquire(Duration.ofSeconds(1))).isTrue();
    }

    @Test
    void givesUpWhenTheSlotFreesAfterTheGrace() {
        var real = new CallRateLimiter(1, Duration.ofSeconds(5), System::nanoTime);
        real.tryAcquire(Duration.ZERO);

        assertThat(real.tryAcquire(Duration.ofMillis(50))).isFalse();
    }
}

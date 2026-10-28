package dev.stockreading.collector.marketdata;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.function.LongSupplier;

/**
 * 슬라이딩 윈도우 호출 상한. 락은 장부를 고치는 짧은 구간에만 잡고, 기다리는 동안에는 잡지 않는다.
 */
final class CallRateLimiter implements CallRateLimit {

    private final int maxCalls;
    private final long windowNanos;
    private final LongSupplier nanoClock;
    private final ArrayDeque<Long> calls = new ArrayDeque<>();

    static CallRateLimiter perSecond(int maxCalls) {
        return new CallRateLimiter(maxCalls, Duration.ofSeconds(1), System::nanoTime);
    }

    CallRateLimiter(int maxCalls, Duration window, LongSupplier nanoClock) {
        this.maxCalls = maxCalls;
        this.windowNanos = window.toNanos();
        this.nanoClock = nanoClock;
    }

    @Override
    public boolean tryAcquire(Duration maxWait) {
        long deadline = nanoClock.getAsLong() + maxWait.toNanos();
        while (true) {
            long waitNanos;
            synchronized (calls) {
                long now = nanoClock.getAsLong();
                while (!calls.isEmpty() && now - calls.peekFirst() >= windowNanos) {
                    calls.pollFirst();
                }
                if (calls.size() < maxCalls) {
                    calls.addLast(now);
                    return true;
                }
                waitNanos = calls.peekFirst() + windowNanos - now;
                if (now + waitNanos > deadline) {
                    return false;
                }
            }
            try {
                Thread.sleep(waitNanos / 1_000_000 + 1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }
}

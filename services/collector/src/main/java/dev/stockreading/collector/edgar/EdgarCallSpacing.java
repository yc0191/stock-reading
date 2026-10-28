package dev.stockreading.collector.edgar;

import java.util.function.LongSupplier;

/**
 * 호출 사이에 최소 간격(1초 / 상한)을 둔다. 간격을 지키면 어느 1초 구간에도 호출이 상한을 넘지 않는다.
 * 기다리는 동안 락을 쥐므로 호출이 한 줄로 선다. 수집은 원래 순차 호출이라 이 편이 단순하다.
 */
final class EdgarCallSpacing {

    interface Sleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    private final long intervalNanos;
    private final LongSupplier nanoClock;
    private final Sleeper sleeper;
    private boolean started;
    private long nextAllowed;

    static EdgarCallSpacing perSecond(int maxCalls) {
        return new EdgarCallSpacing(maxCalls, System::nanoTime,
                nanos -> Thread.sleep(nanos / 1_000_000, (int) (nanos % 1_000_000)));
    }

    EdgarCallSpacing(int maxCallsPerSecond, LongSupplier nanoClock, Sleeper sleeper) {
        // 올림으로 나눠야 간격이 짧아져 상한을 넘는 일이 없다.
        this.intervalNanos = (1_000_000_000L + maxCallsPerSecond - 1) / maxCallsPerSecond;
        this.nanoClock = nanoClock;
        this.sleeper = sleeper;
    }

    synchronized void await() throws InterruptedException {
        long now = nanoClock.getAsLong();
        if (started && now < nextAllowed) {
            sleeper.sleepNanos(nextAllowed - now);
            now = Math.max(nanoClock.getAsLong(), nextAllowed);
        }
        started = true;
        nextAllowed = now + intervalNanos;
    }
}

package dev.stockreading.collector.news;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

class DailyCallLimitTest {

    /** 테스트에서 시각을 직접 옮길 수 있는 시계. */
    private static final class MovableClock extends Clock {
        private Instant now;

        MovableClock(String instant) {
            this.now = Instant.parse(instant);
        }

        void set(String instant) {
            this.now = Instant.parse(instant);
        }

        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    @Test
    void allowsCallsUpToTheLimitThenRefuses() {
        DailyCallLimit limit = new DailyCallLimit(2, new MovableClock("2026-10-22T01:00:00Z"));

        assertThat(limit.tryAcquire()).isTrue();
        assertThat(limit.tryAcquire()).isTrue();
        assertThat(limit.tryAcquire()).isFalse();
        assertThat(limit.tryAcquire()).isFalse();
    }

    @Test
    void resetsAtMidnightKoreaTimeNotUtc() {
        // 한국 시간 2026-10-22 23:59:59 = UTC 14:59:59, 한국 시간 자정 = UTC 15:00:00
        MovableClock clock = new MovableClock("2026-10-22T14:59:59Z");
        DailyCallLimit limit = new DailyCallLimit(1, clock);
        assertThat(limit.tryAcquire()).isTrue();

        clock.set("2026-10-22T15:00:00Z");
        assertThat(limit.tryAcquire()).isTrue();

        // UTC 날짜는 같지만 한국 시간으로는 같은 날이라 다시 거절된다.
        assertThat(limit.tryAcquire()).isFalse();
    }

    @Test
    void sameKoreanDayStaysCountedAcrossUtcMidnight() {
        // 한국 시간 09:00 이전은 UTC 로는 전날이다. UTC 기준으로 세면 여기서 잘못 초기화된다.
        MovableClock clock = new MovableClock("2026-10-21T23:59:59Z");
        DailyCallLimit limit = new DailyCallLimit(1, clock);
        assertThat(limit.tryAcquire()).isTrue();

        clock.set("2026-10-22T00:00:00Z");
        assertThat(limit.tryAcquire()).isFalse();
    }

    @Test
    void refusedCallsAreNotCounted() {
        MovableClock clock = new MovableClock("2026-10-22T01:00:00Z");
        DailyCallLimit limit = new DailyCallLimit(1, clock);
        limit.tryAcquire();
        limit.tryAcquire();
        limit.tryAcquire();

        clock.set("2026-10-22T15:00:00Z");
        assertThat(limit.tryAcquire()).isTrue();
        assertThat(limit.tryAcquire()).isFalse();
    }

    @Test
    void limitBelowOneIsRejectedAtConfigurationTime() {
        assertThatThrownBy(() -> new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, -1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new NewsSearchProperties("u", "id", "secret", "h1", "h2", 20, 5, 1, 0).dailyCallLimit()).isEqualTo(1);
    }
}

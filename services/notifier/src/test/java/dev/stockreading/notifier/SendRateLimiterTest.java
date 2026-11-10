package dev.stockreading.notifier;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SendRateLimiterTest {

    private static final Instant START = Instant.parse("2026-10-20T07:00:00Z");

    private final TestClock clock = new TestClock(START);

    private SendRateLimiter limiter(int maxPerHour) {
        return new SendRateLimiter(new NotifierProperties(false, "a@example.com", "", maxPerHour), clock);
    }

    @Test
    void allowsUpToTheLimitThenBlocks() {
        SendRateLimiter limiter = limiter(3);

        for (int i = 0; i < 3; i++) {
            assertThat(limiter.hasCapacity(1)).as("send #%d", i + 1).isTrue();
            limiter.recordSent(1);
        }

        assertThat(limiter.hasCapacity(1)).isFalse();
    }

    @Test
    void checkingCapacityDoesNotConsumeIt() {
        SendRateLimiter limiter = limiter(1);

        assertThat(limiter.hasCapacity(1)).isTrue();
        assertThat(limiter.hasCapacity(1)).isTrue();
        assertThat(limiter.hasCapacity(1)).isTrue();
    }

    @Test
    void capacityReturnsOnceTheOldestSendIsExactlyOneHourOld() {
        SendRateLimiter limiter = limiter(2);
        limiter.recordSent(1);                       // t = 0
        clock.advance(Duration.ofMinutes(30));
        limiter.recordSent(1);                       // t = 30m
        assertThat(limiter.hasCapacity(1)).isFalse();

        clock.advance(Duration.ofMinutes(29).plusSeconds(59)); // t = 59m59s: 첫 발송은 아직 센다
        assertThat(limiter.hasCapacity(1)).isFalse();

        clock.advance(Duration.ofSeconds(1));       // t = 60m00s: 첫 발송이 정확히 1시간 지났다
        assertThat(limiter.hasCapacity(1)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void aNonPositiveLimitBlocksEverything(int maxPerHour) {
        assertThat(limiter(maxPerHour).hasCapacity(1)).isFalse();
    }

    @Test
    void eachRecipientHasTheirOwnAllowance() {
        SendRateLimiter limiter = limiter(1);
        limiter.recordSent(1);

        assertThat(limiter.hasCapacity(1)).isFalse();
        assertThat(limiter.hasCapacity(2)).isTrue();
    }
}

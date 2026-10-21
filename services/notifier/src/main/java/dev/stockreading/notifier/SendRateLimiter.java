package dev.stockreading.notifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;

import org.springframework.stereotype.Component;

/** 실제로 발송한 이메일에 대한 1시간 슬라이딩 한도. 자동 반복이 실제 받은편지함을 가득 채우지 못하게 한다. */
@Component
public class SendRateLimiter {

    private static final Duration WINDOW = Duration.ofHours(1);

    private final int maxPerHour;
    private final Clock clock;
    private final Deque<Instant> sent = new ArrayDeque<>();

    public SendRateLimiter(NotifierProperties properties, Clock clock) {
        this.maxPerHour = properties.maxPerHour();
        this.clock = clock;
    }

    /** @return 지금 이메일을 더 보낼 수 있으면 true. 이 호출 자체는 횟수를 세지 않는다. */
    public synchronized boolean hasCapacity() {
        purge();
        return sent.size() < maxPerHour;
    }

    /** 이메일을 실제로 발송한 뒤에 호출한다. */
    public synchronized void recordSent() {
        sent.addLast(clock.instant());
    }

    private void purge() {
        Instant oldestAllowed = clock.instant().minus(WINDOW);
        // 정확히 1시간 지난 항목은 더 이상 세지 않는다.
        while (!sent.isEmpty() && !sent.peekFirst().isAfter(oldestAllowed)) {
            sent.removeFirst();
        }
    }
}

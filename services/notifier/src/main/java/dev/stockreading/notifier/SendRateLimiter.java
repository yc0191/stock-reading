package dev.stockreading.notifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

/** 받는 사람(메일 번호) 한 명당, 실제로 발송한 급변동 이메일에 대한 1시간 슬라이딩 한도. 자동 반복이 실제 받은편지함을 가득 채우지 못하게 한다. */
@Component
class SendRateLimiter {

    private static final Duration WINDOW = Duration.ofHours(1);

    private final int maxPerHour;
    private final Clock clock;
    private final Map<Long, Deque<Instant>> sentByMail = new HashMap<>();

    public SendRateLimiter(NotifierProperties properties, Clock clock) {
        this.maxPerHour = properties.maxPerHour();
        this.clock = clock;
    }

    /** @return 이 받는 사람에게 지금 이메일을 더 보낼 수 있으면 true. 이 호출 자체는 횟수를 세지 않는다. */
    public synchronized boolean hasCapacity(long mailId) {
        Deque<Instant> sent = purge(mailId);
        return sent == null ? maxPerHour > 0 : sent.size() < maxPerHour;
    }

    /** 이 받는 사람에게 이메일을 실제로 발송한 뒤에 호출한다. */
    public synchronized void recordSent(long mailId) {
        sentByMail.computeIfAbsent(mailId, id -> new ArrayDeque<>()).addLast(clock.instant());
    }

    /** 1시간이 지난 기록을 지우고, 남은 기록이 없으면 항목을 없앤다(받는 사람마다 메모리가 계속 남지 않게). */
    private Deque<Instant> purge(long mailId) {
        Deque<Instant> sent = sentByMail.get(mailId);
        if (sent == null) {
            return null;
        }
        Instant oldestAllowed = clock.instant().minus(WINDOW);
        // 정확히 1시간 지난 항목은 더 이상 세지 않는다.
        while (!sent.isEmpty() && !sent.peekFirst().isAfter(oldestAllowed)) {
            sent.removeFirst();
        }
        if (sent.isEmpty()) {
            sentByMail.remove(mailId);
            return null;
        }
        return sent;
    }
}

package dev.stockreading.collector.edgar;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/** 거래소 정보를 submissions 에서 읽어 하루 동안 기억한다. 상장 거래소는 거의 바뀌지 않아 매번 조회할 필요가 없다. */
@Component
class EdgarExchanges implements ExchangePort {

    static final Duration TTL = Duration.ofHours(24);

    private final EdgarPort edgar;
    private final Clock clock;
    private final Map<String, Cached> byTicker = new ConcurrentHashMap<>();

    EdgarExchanges(EdgarPort edgar, Clock clock) {
        this.edgar = edgar;
        this.clock = clock;
    }

    @Override
    public List<String> exchanges(String ticker) {
        String key = EdgarClient.normalize(ticker);
        Instant now = clock.instant();
        Cached cached = byTicker.get(key);
        if (cached != null && now.isBefore(cached.fetchedAt().plus(TTL))) {
            return cached.exchanges();
        }
        List<String> exchanges = List.copyOf(edgar.submissions(edgar.cikOf(ticker)).exchanges());
        byTicker.put(key, new Cached(exchanges, now));
        return exchanges;
    }

    private record Cached(List<String> exchanges, Instant fetchedAt) {
    }
}

package dev.stockreading.collector.schedule;

import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.edgar.CollectedFiling;
import dev.stockreading.collector.edgar.EdgarApiException;
import dev.stockreading.collector.edgar.Filing;
import dev.stockreading.collector.edgar.FilingCollector;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.event.FilingPayload;
import tools.jackson.databind.ObjectMapper;

/** 수집한 공시를 공시마다 filing.collected 이벤트 한 건으로 발행한다. 메시지 키는 접수번호다. */
@Component
class FilingPublisher {

    static final String EVENT_TYPE = "filing.collected";

    private static final Logger log = LoggerFactory.getLogger(FilingPublisher.class);
    private static final int MAX_REMEMBERED_KEYS = 10_000;
    private static final int MAX_FORM = 20;
    private static final Pattern TICKER = Pattern.compile("^[A-Z0-9]{1,6}([.-][A-Z0-9]{1,2})?$");
    private static final Pattern CIK = Pattern.compile("^[0-9]{10}$");
    private static final Pattern ACCESSION = Pattern.compile("^[0-9]{10}-[0-9]{2}-[0-9]{6}$");

    private final FilingCollector collector;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    // 메모리 기반 재발행 억제이므로 재시작하면 수집 범위(lookback) 안의 공시가 다시 발행된다.
    // 중복은 소비 쪽이 접수번호로 최종으로 막는다. 같은 공시라도 보유 티커 조합이 바뀌면 새 연결을
    // 알리기 위해 다시 발행한다(뉴스 수집의 재발행 억제와 같은 방식).
    private final Set<SentKey> sentKeys = ConcurrentHashMap.newKeySet();

    FilingPublisher(FilingCollector collector, KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.collector = collector;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    void publishCollected(String correlationId) {
        List<CollectedFiling> filings;
        try {
            filings = collector.collect();
        } catch (EdgarApiException e) {
            // 메시지는 실패 종류와 HTTP 상태만 담는다(User-Agent, 티커 없음).
            log.warn("filing collect failed corr={} reason={}", correlationId, e.getMessage());
            return;
        } catch (RuntimeException e) {
            log.warn("filing collect failed corr={} errorType={}", correlationId, e.getClass().getSimpleName());
            return;
        }

        int published = 0;
        int alreadyPublished = 0;
        int invalid = 0;
        int sendFailed = 0;
        for (CollectedFiling collected : filings) {
            FilingPayload payload = toPayload(collected);
            if (payload == null) {
                invalid++;
                continue;
            }
            SentKey sentKey = SentKey.of(payload);
            if (sentKeys.contains(sentKey)) {
                alreadyPublished++;
                continue;
            }
            CompletableFuture<SendResult<String, String>> future;
            try {
                Envelope<FilingPayload> event = EventFactory.create(
                        clock, EVENT_TYPE, collected.filing().acceptedAt(), correlationId, payload);
                future = kafka.send(Topics.FILING_COLLECTED, payload.accessionNumber(), mapper.writeValueAsString(event));
            } catch (RuntimeException e) {
                sendFailed++;
                log.warn("filing publish failed corr={} errorType={}", correlationId, e.getClass().getSimpleName());
                continue;
            }
            // 전송이 끝난 뒤에만 기록해야, 비동기로 실패한 공시가 다음 tick 에서 다시 발행된다.
            future.whenComplete((result, error) -> {
                if (error == null) {
                    remember(sentKey);
                } else {
                    log.warn("filing send failed corr={} errorType={}", correlationId, error.getClass().getSimpleName());
                }
            });
            published++;
        }

        log.info("sent {} filing.collected events corr={} collected={} alreadyPublished={} invalid={} sendFailed={}",
                published, correlationId, filings.size(), alreadyPublished, invalid, sendFailed);
    }

    private void remember(SentKey key) {
        if (sentKeys.size() >= MAX_REMEMBERED_KEYS) {
            sentKeys.clear();
        }
        sentKeys.add(key);
    }

    private record SentKey(String accessionNumber, List<String> tickers) {

        static SentKey of(FilingPayload payload) {
            return new SentKey(payload.accessionNumber(), payload.tickers().stream().sorted().toList());
        }
    }

    // 스키마를 어기는 공시는 null 을 돌려 발행에서 뺀다.
    private static FilingPayload toPayload(CollectedFiling collected) {
        Filing filing = collected.filing();
        List<String> tickers = collected.tickers() == null ? List.of() : collected.tickers().stream().distinct().toList();
        if (tickers.isEmpty() || !tickers.stream().allMatch(t -> t != null && TICKER.matcher(t).matches())) {
            return null;
        }
        if (collected.cik() == null || !CIK.matcher(collected.cik()).matches()
                || filing.accessionNumber() == null || !ACCESSION.matcher(filing.accessionNumber()).matches()) {
            return null;
        }
        if (filing.form() == null || filing.form().isBlank() || filing.form().length() > MAX_FORM
                || filing.acceptedAt() == null || filing.filingDate() == null || !isUri(filing.url())) {
            return null;
        }
        List<String> items = filing.items() == null ? List.of() : filing.items().stream().distinct().toList();
        return new FilingPayload(
                tickers,
                collected.cik(),
                filing.accessionNumber(),
                filing.form(),
                items,
                EventFactory.format(filing.acceptedAt()),
                filing.filingDate().toString(),
                filing.url());
    }

    private static boolean isUri(String text) {
        try {
            return text != null && URI.create(text).isAbsolute();
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}

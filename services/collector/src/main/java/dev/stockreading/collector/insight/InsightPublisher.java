package dev.stockreading.collector.insight;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.marketdata.BrokerApiException;
import tools.jackson.databind.ObjectMapper;

/**
 * 일봉(종목별), 세계지수(대륙별), 환율종합, 시장종합 순으로 한 번씩 조회해 응답마다 market.insight 한 건을
 * 발행한다. 한 건이 실패해도 나머지는 계속한다. 메시지 키는 "<apiType>:<target>"이고 target 이 없으면 apiType 이다.
 */
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
class InsightPublisher {

    static final String EVENT_TYPE = "market.insight";
    static final String DAILY_PRICE = "daily-price";
    static final String WORLD_INDEX = "world-index";
    static final String EXCHANGE_RATE_SUMMARY = "exchange-rate-summary";
    static final String MARKET_SUMMARY = "market-summary";

    private static final Logger log = LoggerFactory.getLogger(InsightPublisher.class);
    private static final Pattern SYMBOL = Pattern.compile("^[0-9A-Z]{6}$");

    private final InsightSourcePort source;
    private final CollectorProperties collector;
    private final InsightProperties properties;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    InsightPublisher(InsightSourcePort source, CollectorProperties collector, InsightProperties properties,
                     KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.source = source;
        this.collector = collector;
        this.properties = properties;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    void publishAll(String correlationId, String scheduledFor) {
        int published = 0;
        int failed = 0;
        for (String symbol : collector.symbols()) {
            if (!SYMBOL.matcher(symbol).matches()) {
                // 스키마를 어기는 종목코드는 조회하지 않고 건너뛴다.
                log.warn("insight skipped apiType={} reason=invalid-target corr={}", DAILY_PRICE, correlationId);
                failed++;
                continue;
            }
            if (publishOne(DAILY_PRICE, symbol, () -> source.dailyPrice(symbol), correlationId, scheduledFor)) {
                published++;
            } else {
                failed++;
            }
        }
        List<String> continents = properties.continents() == null ? List.of() : properties.continents();
        for (String continent : continents) {
            if (publishOne(WORLD_INDEX, continent, () -> source.worldIndex(continent), correlationId, scheduledFor)) {
                published++;
            } else {
                failed++;
            }
        }
        if (publishOne(EXCHANGE_RATE_SUMMARY, null, source::exchangeRateSummary, correlationId, scheduledFor)) {
            published++;
        } else {
            failed++;
        }
        if (publishOne(MARKET_SUMMARY, null, source::marketSummary, correlationId, scheduledFor)) {
            published++;
        } else {
            failed++;
        }
        log.info("sent {} market.insight events corr={} failed={}", published, correlationId, failed);
    }

    private boolean publishOne(String apiType, String target, Supplier<String> fetch,
                               String correlationId, String scheduledFor) {
        String response;
        try {
            response = fetch.get();
        } catch (RuntimeException e) {
            // 서버 문구는 남기지 않는다. 클라이언트가 만든 코드 형식의 메시지(BrokerApiException)만 남기고
            // 그 밖의 예외는 종류만 남긴다.
            String reason = e instanceof BrokerApiException ? e.getMessage() : e.getClass().getSimpleName();
            log.warn("insight fetch failed apiType={} target={} corr={} reason={}",
                    apiType, target, correlationId, reason);
            return false;
        }
        if (response == null || response.isBlank()) {
            log.warn("insight fetch empty apiType={} target={} corr={}", apiType, target, correlationId);
            return false;
        }
        try {
            OffsetDateTime fetchedAt = OffsetDateTime.now(clock);
            Envelope<InsightPayload> event = EventFactory.create(
                    clock,
                    EVENT_TYPE,
                    fetchedAt,
                    correlationId,
                    new InsightPayload(apiType, target, response.trim(), scheduledFor, EventFactory.format(fetchedAt)));
            String key = target == null ? apiType : apiType + ":" + target;
            kafka.send(Topics.MARKET_INSIGHT, key, mapper.writeValueAsString(event))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.warn("insight send failed apiType={} target={} corr={} errorType={}",
                                    apiType, target, correlationId, error.getClass().getSimpleName());
                        }
                    });
            return true;
        } catch (RuntimeException e) {
            log.warn("insight publish failed apiType={} target={} corr={} errorType={}",
                    apiType, target, correlationId, e.getClass().getSimpleName());
            return false;
        }
    }
}

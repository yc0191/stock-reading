package dev.stockreading.collector.insight;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.holdings.QuoteSymbolsPort;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.edgar.ExchangePort;
import dev.stockreading.collector.event.Envelope;
import dev.stockreading.collector.event.EventFactory;
import dev.stockreading.collector.holdings.HoldingsPort;
import dev.stockreading.collector.marketdata.BrokerApiException;
import tools.jackson.databind.ObjectMapper;

/**
 * 장 마감 후에는 일봉(종목별), 세계지수(대륙별), 환율종합, 시장종합을, 장 시작 전에는 세계지수 아메리카와
 * 해외 일봉(보유 미국 종목별)을 한 번씩 조회해 응답마다 market.insight 한 건을 발행한다. 한 건이 실패해도
 * 나머지는 계속한다. 메시지 키는 "<apiType>:<target>"이고 target 이 없으면 apiType 이다.
 */
@Component
@ConditionalOnProperty(name = "collector.market-data.source", havingValue = "broker")
public class InsightPublisher {

    static final String EVENT_TYPE = "market.insight";
    // 아래 자료 종류 이름(apiType)은 데일리 판 시작 알림(daily.collection.completed)의 missingSources 에도 쓴다.
    public static final String DAILY_PRICE = "daily-price";
    public static final String WORLD_INDEX = "world-index";
    public static final String EXCHANGE_RATE_SUMMARY = "exchange-rate-summary";
    public static final String MARKET_SUMMARY = "market-summary";
    public static final String OVERSEAS_DAILY_PRICE = "overseas-daily-price";
    // 해외 일봉 실패 종류 중 시세 호출 전에 난 것. 시세 호출 실패는 FailureType 이름을 쓴다.
    static final String EXCHANGE_LOOKUP_FAILED = "EXCHANGE_LOOKUP_FAILED";
    static final String EXCHANGE_UNKNOWN = "EXCHANGE_UNKNOWN";
    static final String EMPTY_RESPONSE = "EMPTY_RESPONSE";
    static final String PUBLISH_FAILED = "PUBLISH_FAILED";
    // 장 시작 전에는 막 끝난 미국 장의 지수만 필요하므로 아메리카만 조회한다.
    static final String PRE_OPEN_CONTINENT = "C";

    private static final Logger log = LoggerFactory.getLogger(InsightPublisher.class);
    private static final Pattern SYMBOL = Pattern.compile("^[0-9A-Z]{6}$");

    private final InsightSourcePort source;
    private final QuoteSymbolsPort quoteSymbols;
    private final InsightProperties properties;
    private final HoldingsPort holdings;
    private final ExchangePort exchanges;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    InsightPublisher(InsightSourcePort source, QuoteSymbolsPort quoteSymbols, InsightProperties properties,
                     HoldingsPort holdings, ExchangePort exchanges,
                     KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.source = source;
        this.quoteSymbols = quoteSymbols;
        this.properties = properties;
        this.holdings = holdings;
        this.exchanges = exchanges;
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** 마감 후 조회. 하나라도 받지 못한 자료 종류(apiType)를 돌려준다(데일리 판 시작 알림의 missingSources). */
    public Set<String> publishAll(String correlationId, String scheduledFor) {
        int published = 0;
        int failed = 0;
        Set<String> missing = new LinkedHashSet<>();
        for (String symbol : quoteSymbols.symbols()) {
            if (!SYMBOL.matcher(symbol).matches()) {
                // 스키마를 어기는 종목코드는 조회하지 않고 건너뛴다.
                log.warn("insight skipped apiType={} reason=invalid-target corr={}", DAILY_PRICE, correlationId);
                failed++;
                missing.add(DAILY_PRICE);
                continue;
            }
            if (publishOne(DAILY_PRICE, symbol, () -> source.dailyPrice(symbol), correlationId, scheduledFor)) {
                published++;
            } else {
                failed++;
                missing.add(DAILY_PRICE);
            }
        }
        List<String> continents = properties.continents() == null ? List.of() : properties.continents();
        for (String continent : continents) {
            if (publishOne(WORLD_INDEX, continent, () -> source.worldIndex(continent), correlationId, scheduledFor)) {
                published++;
            } else {
                failed++;
                missing.add(WORLD_INDEX);
            }
        }
        if (publishOne(EXCHANGE_RATE_SUMMARY, null, source::exchangeRateSummary, correlationId, scheduledFor)) {
            published++;
        } else {
            failed++;
            missing.add(EXCHANGE_RATE_SUMMARY);
        }
        if (publishOne(MARKET_SUMMARY, null, source::marketSummary, correlationId, scheduledFor)) {
            published++;
        } else {
            failed++;
            missing.add(MARKET_SUMMARY);
        }
        log.info("sent {} market.insight events corr={} failed={}", published, correlationId, failed);
        // 해외 일봉은 장 시작 전에만 받는다. 국내 장 마감 무렵에는 미국 장이 열리지 않아 같은 값을 다시 받게 된다.
        return missing;
    }

    /**
     * 장 시작 전 조회: 세계지수 아메리카와 보유 미국 종목 해외 일봉. 이벤트와 키는 장 마감 후 조회와 같다.
     * 하나라도 받지 못한 자료 종류(apiType)를 돌려준다(데일리 판 시작 알림의 missingSources).
     */
    public Set<String> publishPreOpen(String correlationId, String scheduledFor) {
        Set<String> missing = new LinkedHashSet<>();
        boolean published = publishOne(WORLD_INDEX, PRE_OPEN_CONTINENT,
                () -> source.worldIndex(PRE_OPEN_CONTINENT), correlationId, scheduledFor);
        log.info("sent {} pre-open world index events corr={} failed={}",
                published ? 1 : 0, correlationId, published ? 0 : 1);
        if (!published) {
            missing.add(WORLD_INDEX);
        }
        if (!publishOverseas(correlationId, scheduledFor)) {
            missing.add(OVERSEAS_DAILY_PRICE);
        }
        return missing;
    }

    // 종목 수만큼 상한 장치를 기다리며 차례로 호출하므로 세계지수 조회가 늦어지지 않게 맨 뒤에 둔다.
    // 티커는 보유 종목이 드러나므로 로그에는 순번과 실패 종류, 개수만 남긴다.
    // 상한을 넘어 건너뛴 종목이나 실패한 종목이 있으면 false.
    private boolean publishOverseas(String correlationId, String scheduledFor) {
        List<String> tickers = holdings.allTickers();
        int limit = properties.overseasMaxTickers();
        boolean skipped = tickers.size() > limit;
        if (skipped) {
            log.warn("overseas daily price skipped {} tickers over the limit {} corr={}",
                    tickers.size() - limit, limit, correlationId);
            tickers = tickers.subList(0, limit);
        }
        int published = 0;
        Map<String, Integer> failures = new TreeMap<>();
        for (int i = 0; i < tickers.size(); i++) {
            String failure = publishOverseasOne(i + 1, tickers.get(i), correlationId, scheduledFor);
            if (failure == null) {
                published++;
            } else {
                failures.merge(failure, 1, Integer::sum);
            }
        }
        int failed = failures.values().stream().mapToInt(Integer::intValue).sum();
        log.info("sent {} overseas daily price events corr={} tickers={} failed={} failures={}",
                published, correlationId, tickers.size(), failed, failures);
        return !skipped && failed == 0;
    }

    /** 성공하면 null, 실패하면 실패 종류. */
    private String publishOverseasOne(int position, String ticker, String correlationId, String scheduledFor) {
        String code;
        try {
            code = OverseasExchange.codeOf(exchanges.exchanges(ticker));
        } catch (RuntimeException e) {
            log.warn("overseas daily price failed position={} reason={} errorType={} corr={}",
                    position, EXCHANGE_LOOKUP_FAILED, e.getClass().getSimpleName(), correlationId);
            return EXCHANGE_LOOKUP_FAILED;
        }
        if (code == null) {
            log.warn("overseas daily price failed position={} reason={} corr={}", position, EXCHANGE_UNKNOWN, correlationId);
            return EXCHANGE_UNKNOWN;
        }
        String response;
        try {
            response = source.overseasDailyPrice(code, ticker);
        } catch (RuntimeException e) {
            String reason = e instanceof BrokerApiException broker ? broker.failureType().name() : e.getClass().getSimpleName();
            // BrokerApiException 메시지는 우리가 만든 문구와 처리 코드뿐이라 남긴다. 그 밖의 예외는 종류만 남긴다.
            String detail = e instanceof BrokerApiException ? e.getMessage() : "";
            log.warn("overseas daily price failed position={} reason={} detail={} corr={}",
                    position, reason, detail, correlationId);
            return reason;
        }
        if (response == null || response.isBlank()) {
            log.warn("overseas daily price failed position={} reason={} corr={}", position, EMPTY_RESPONSE, correlationId);
            return EMPTY_RESPONSE;
        }
        return send(OVERSEAS_DAILY_PRICE, ticker, "position=" + position, response, correlationId, scheduledFor)
                ? null : PUBLISH_FAILED;
    }

    // 장중 조회(IntradayPublisher)도 같은 실패 처리와 발행을 쓰도록 패키지 안에 연다.
    boolean publishOne(String apiType, String target, Supplier<String> fetch,
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
        return send(apiType, target, "target=" + target, response, correlationId, scheduledFor);
    }

    // logLabel 은 로그에서 대상을 가리키는 말이다. 보유 종목처럼 값을 남기면 안 되는 대상은 순번을 넘긴다.
    private boolean send(String apiType, String target, String logLabel, String response,
                         String correlationId, String scheduledFor) {
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
                            log.warn("insight send failed apiType={} {} corr={} errorType={}",
                                    apiType, logLabel, correlationId, error.getClass().getSimpleName());
                        }
                    });
            return true;
        } catch (RuntimeException e) {
            log.warn("insight publish failed apiType={} {} corr={} errorType={}",
                    apiType, logLabel, correlationId, e.getClass().getSimpleName());
            return false;
        }
    }
}

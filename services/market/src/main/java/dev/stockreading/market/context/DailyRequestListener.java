package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.holdings.HoldingsPort;
import dev.stockreading.market.insight.OverseasCloseRateQuery;
import tools.jackson.databind.ObjectMapper;

/**
 * 데일리 판 하나에 쓸 관심 ETF 전체의 하루 등락, 담긴 미국 종목과 직전 정규장 등락률, 시장 배경을 한 번에 답한다.
 *
 * <p>ETF 목록은 종목 테이블의 국내 ETF 중 관계가 HELD 또는 WATCH인 종목이다. 받은 시각이 [하한, 기준 시각] 안인
 * 국내 일봉(daily-price)이 있으면 값을 싣고, 없거나 읽지 못하면 값을 비우고 이유를 싣는다. 미국 종목은 대응표로 찾고, 해외 등락률·세계지수 계산·받은 시각 하한은
 * 배경 자료 응답(market.context)과 같은 규칙을 쓴다. 저장 상태를 바꾸지 않는 조회라 같은 요청을 다시 받아도
 * 같은 내용으로 다시 답할 뿐이고, 브리핑은 jobId·seq로 짝을 지어 중복 응답을 버린다.
 */
@Component
class DailyRequestListener {

    private static final Logger log = LoggerFactory.getLogger(DailyRequestListener.class);

    private final BackgroundRepository repository;
    private final MarketBackground marketBackground;
    private final DailyPriceReader dailyPriceReader;
    private final OverseasCloseRateQuery closeRateQuery;
    private final HoldingsPort holdings;
    private final ContextProperties contextProperties;
    private final DailyProperties dailyProperties;
    private final DailyPublisher publisher;
    private final ObjectMapper mapper;

    DailyRequestListener(BackgroundRepository repository, WorldIndexRates worldIndexRates,
            OverseasCloseRateQuery closeRateQuery, HoldingsPort holdings, ContextProperties contextProperties,
            DailyProperties dailyProperties, DailyPublisher publisher, ObjectMapper mapper) {
        this.repository = repository;
        this.marketBackground = new MarketBackground(repository, worldIndexRates);
        this.dailyPriceReader = new DailyPriceReader(mapper);
        this.closeRateQuery = closeRateQuery;
        this.holdings = holdings;
        this.contextProperties = contextProperties;
        this.dailyProperties = dailyProperties;
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.MARKET_DAILY_REQUESTED, groupId = "market")
    void onRequest(String message) {
        DailyRequestEnvelope event = mapper.readValue(message, DailyRequestEnvelope.class);
        publisher.publish(answer(event.payload()), event.eventId(), event.correlationId());
    }

    DailyPayload answer(DailyRequestEnvelope.RequestPayload request) {
        OffsetDateTime asOf = OffsetDateTime.parse(request.asOf());
        OffsetDateTime requestedSince = OffsetDateTime.parse(request.since());
        if (request.maxResponseBytes() < 1 || request.maxEtfs() < 1 || request.maxTickers() < 1
                || requestedSince.isAfter(asOf)) {
            // 잘못된 요청은 응답하지 않는다. 브리핑이 제한 시간 뒤 다시 보내거나 실패로 기록한다.
            throw new IllegalArgumentException("invalid daily request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }
        OffsetDateTime floor = asOf.minus(contextProperties.maxAge());
        OffsetDateTime since = requestedSince.isAfter(floor) ? requestedSince : floor;
        int maxBytes = Math.min(request.maxResponseBytes(), contextProperties.maxResponseBytes());
        int maxEtfs = Math.min(request.maxEtfs(), dailyProperties.maxEtfs());
        int maxTickers = Math.min(request.maxTickers(), contextProperties.maxTickers());

        // 받은 시각이 범위 안인 일봉을 종목별로 모아 두고, ETF 목록은 종목 테이블에서 만든다. 일봉만 있고 테이블에 없는 종목은 쓰지 않는다.
        Map<String, BackgroundRepository.StoredResponse> dailyBySymbol = new HashMap<>();
        for (BackgroundRepository.StoredTargetResponse stored : repository.latestDailyPrices(since, asOf)) {
            // 저장소가 기간을 지키더라도 여기서 한 번 더 확인한다. 어기면 조용히 미래·오래된 값이 나간다.
            OffsetDateTime fetchedAt = stored.row().fetchedAt();
            if (!fetchedAt.isBefore(since) && !fetchedAt.isAfter(asOf)) {
                dailyBySymbol.put(stored.target(), stored.row());
            }
        }
        List<DailyPayload.Etf> etfs = new ArrayList<>();
        int noDaily = 0;
        int unreadable = 0;
        List<String> tracked = holdings.trackedEtfCodes();
        // 잘릴 ETF는 미국 종목과 ETF 정보를 조회하지 않도록 상한을 먼저 적용한다.
        boolean truncated = tracked.size() > maxEtfs;
        for (String symbol : tracked.stream().limit(maxEtfs).toList()) {
            BackgroundRepository.StoredResponse stored = dailyBySymbol.get(symbol);
            Optional<DailyPriceReader.Record> record =
                    stored == null ? Optional.empty() : dailyPriceReader.latest(stored.response());
            if (stored == null) {
                noDaily++;
            } else if (record.isEmpty()) {
                unreadable++;
            }
            etfs.add(etf(symbol, record.orElse(null), stored, stored == null
                    ? DailyPayload.NO_DAILY_PRICE_IN_RANGE : DailyPayload.DAILY_PRICE_UNREADABLE, maxTickers));
        }
        if (noDaily > 0 || unreadable > 0) {
            // 종목코드는 보유 종목이 드러나므로 건수만 남긴다.
            log.warn("etfs without daily price jobId={} noDailyInRange={} unreadable={}", request.jobId(), noDaily,
                    unreadable);
        }

        Set<String> tickers = new LinkedHashSet<>();
        etfs.forEach(etf -> tickers.addAll(etf.tickers()));
        List<ContextPayload.CloseRate> closeRates = new ArrayList<>();
        for (String ticker : tickers) {
            closeRateQuery.find(ticker, asOf, since)
                    .ifPresent(rate -> closeRates.add(MarketBackground.closeRate(ticker, rate)));
        }

        List<ContextPayload.Background> background =
                marketBackground.market(contextProperties.worldIndexContinents(), since, asOf, maxBytes);
        return new DailyPayload(request.jobId(), request.seq(), etfs, closeRates, background, truncated);
    }

    /** record가 null이면 일봉 값을 비우고 missingReason을 싣는다. 미국 종목과 ETF 정보는 일봉과 관계없이 싣는다. */
    private DailyPayload.Etf etf(String symbol, DailyPriceReader.Record record,
            BackgroundRepository.StoredResponse stored, String missingReason, int maxTickers) {
        List<String> allTickers = holdings.topTickers(symbol);
        List<String> tickers = allTickers.stream().limit(maxTickers).toList();
        boolean tickersTruncated = allTickers.size() > tickers.size();
        ContextPayload.EtfInfo info =
                holdings.etfInfo(symbol).map(i -> MarketBackground.etfInfo(i, maxTickers)).orElse(null);
        if (record == null) {
            return DailyPayload.Etf.withoutDaily(symbol, missingReason, tickers, tickersTruncated, info);
        }
        return new DailyPayload.Etf(symbol, record.date(), record.close(), record.prevClose(), record.rate(),
                EventFactory.formatKst(stored.fetchedAt()), tickers, tickersTruncated, info);
    }
}

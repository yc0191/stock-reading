package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
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
 * 데일리 판 하나에 쓸 보유 ETF 전체의 하루 등락, 담긴 미국 종목과 직전 정규장 등락률, 시장 배경을 한 번에 답한다.
 *
 * <p>보유 ETF는 받은 시각이 [하한, 기준 시각] 안인 국내 일봉(daily-price)이 저장된 종목이다. 보유 종목 설정이 따로 없어
 * 수집기가 일봉을 받는 종목을 그대로 따른다. 미국 종목은 대응표로 찾고, 해외 등락률·세계지수 계산·받은 시각 하한은
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

        List<DailyPayload.Etf> etfs = new ArrayList<>();
        int unreadable = 0;
        for (BackgroundRepository.StoredTargetResponse stored : repository.latestDailyPrices(since, asOf)) {
            // 저장소가 기간을 지키더라도 여기서 한 번 더 확인한다. 어기면 조용히 미래·오래된 값이 나간다.
            OffsetDateTime fetchedAt = stored.row().fetchedAt();
            if (fetchedAt.isBefore(since) || fetchedAt.isAfter(asOf)) {
                continue;
            }
            Optional<DailyPriceReader.Record> record = dailyPriceReader.latest(stored.row().response());
            if (record.isEmpty()) {
                unreadable++;
                continue;
            }
            etfs.add(etf(stored.target(), record.get(), fetchedAt, maxTickers));
        }
        if (unreadable > 0) {
            // 종목코드는 보유 종목이 드러나므로 건수만 남긴다.
            log.warn("daily-price rows skipped: no readable record jobId={} count={}", request.jobId(), unreadable);
        }
        boolean truncated = etfs.size() > maxEtfs;
        List<DailyPayload.Etf> kept = etfs.stream().limit(maxEtfs).toList();

        Set<String> tickers = new LinkedHashSet<>();
        kept.forEach(etf -> tickers.addAll(etf.tickers()));
        List<ContextPayload.CloseRate> closeRates = new ArrayList<>();
        for (String ticker : tickers) {
            closeRateQuery.find(ticker, asOf, since)
                    .ifPresent(rate -> closeRates.add(MarketBackground.closeRate(ticker, rate)));
        }

        List<ContextPayload.Background> background =
                marketBackground.market(contextProperties.worldIndexContinents(), since, asOf, maxBytes);
        return new DailyPayload(request.jobId(), request.seq(), kept, closeRates, background, truncated);
    }

    private DailyPayload.Etf etf(String symbol, DailyPriceReader.Record record, OffsetDateTime fetchedAt,
            int maxTickers) {
        List<String> allTickers = holdings.topTickers(symbol);
        List<String> tickers = allTickers.stream().limit(maxTickers).toList();
        return new DailyPayload.Etf(symbol, record.date(), record.close(), record.prevClose(), record.rate(),
                EventFactory.formatKst(fetchedAt), tickers, allTickers.size() > tickers.size(),
                holdings.etfInfo(symbol).map(info -> MarketBackground.etfInfo(info, maxTickers)).orElse(null));
    }
}

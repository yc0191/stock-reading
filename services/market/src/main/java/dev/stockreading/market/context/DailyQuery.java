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
import org.springframework.stereotype.Component;

import dev.stockreading.market.MarketProperties;
import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.constituent.HoldingsPort;
import dev.stockreading.market.members.MemberChanges;
import dev.stockreading.market.members.MemberChangesPort;
import dev.stockreading.market.insight.OverseasCloseRateQuery;
import tools.jackson.databind.ObjectMapper;

/**
 * 데일리 판 하나에 쓸 관심 ETF 전체의 하루 등락, 담긴 미국 종목과 직전 정규장 등락률, 시장 배경을 한 번에 답한다.
 *
 * <p>ETF 목록은 종목 테이블의 국내 ETF 중 관계가 HELD 또는 WATCH인 종목이다. 받은 시각이 [하한, 기준 시각] 안인
 * 국내 일봉(daily-price)이 있으면 값을 싣고, 없거나 읽지 못하면 값을 비우고 이유를 싣되, 받은 시각이 같은 범위 안인 장중 시세가
 * 있으면 그 시세로 계산한 하루 등락 대체값을 싣는다. 미국 종목은 대응표로 찾고, 해외 등락률·세계지수 계산·받은 시각 하한은
 * 배경 자료 응답(market.context)과 같은 규칙을 쓴다. 저장 상태를 바꾸지 않는 조회라 같은 요청을 다시 받아도
 * 같은 내용으로 다시 답할 뿐이다(브리핑은 연결 실패·시간 초과 때 같은 요청을 다시 부른다).
 */
@Component
class DailyQuery {

    private static final Logger log = LoggerFactory.getLogger(DailyQuery.class);

    private final BackgroundRepository repository;
    private final MarketBackground marketBackground;
    private final DailyPriceReader dailyPriceReader;
    private final OverseasCloseRateQuery closeRateQuery;
    private final HoldingsPort holdings;
    private final MemberChangesPort memberChanges;
    private final ContextProperties contextProperties;
    private final DailyProperties dailyProperties;
    private final OperationsReader operationsReader;
    private final OpenFlowCalculator openFlow;
    private final DayRateFallback dayRateFallback;

    DailyQuery(BackgroundRepository repository, WorldIndexRates worldIndexRates,
            OverseasCloseRateQuery closeRateQuery, HoldingsPort holdings, MemberChangesPort memberChanges,
            ContextProperties contextProperties,
            DailyProperties dailyProperties, OperationsReader operationsReader,
            ObjectMapper mapper, OpenFlowPort openFlowPort, IntradayQuotePort intradayQuotes,
            MarketProperties marketProperties) {
        this.repository = repository;
        this.marketBackground = new MarketBackground(repository, worldIndexRates,
                contextProperties.worldIndexPrevLookback());
        this.dailyPriceReader = new DailyPriceReader(mapper);
        this.closeRateQuery = closeRateQuery;
        this.holdings = holdings;
        this.memberChanges = memberChanges;
        this.contextProperties = contextProperties;
        this.dailyProperties = dailyProperties;
        this.operationsReader = operationsReader;
        this.openFlow = new OpenFlowCalculator(openFlowPort, mapper, marketProperties.openPriceFields(),
                marketProperties.prevCloseFields());
        this.dayRateFallback = new DayRateFallback(intradayQuotes, mapper, marketProperties.prevCloseFields());
    }

    /** 내부 조회 API 가 부른다. 같은 요청에는 같은 내용을 돌려준다. */
    DailyPayload answer(DailyRequest request) {
        OffsetDateTime asOf = OffsetDateTime.parse(request.asOf());
        OffsetDateTime requestedSince = OffsetDateTime.parse(request.since());
        if (request.maxResponseBytes() < 1 || request.maxEtfs() < 1 || request.maxTickers() < 1
                || requestedSince.isAfter(asOf)) {
            // 잘못된 요청은 답하지 않는다. 내부 조회 API 는 먼저 400 으로 거른다.
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
        int fallbacks = 0;
        List<String> tracked = holdings.trackedEtfs();
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
            DailyPayload.Etf etf = etf(symbol, record.orElse(null), stored, stored == null
                    ? DailyPayload.NO_DAILY_PRICE_IN_RANGE : DailyPayload.DAILY_PRICE_UNREADABLE, maxTickers, since,
                    asOf);
            // 아침 판은 대체값을 쓰지 않으므로 요청한 판에서만 장중 시세를 읽는다.
            if (record.isEmpty() && request.wantsIntradayFallback()) {
                etf = withFallback(etf, since, asOf);
                if (etf.intradayFallback() != null) {
                    fallbacks++;
                }
            }
            etfs.add(etf);
        }
        if (noDaily > 0 || unreadable > 0) {
            // 종목코드는 보유 종목이 드러나므로 건수만 남긴다.
            log.warn("etfs without daily price jobId={} noDailyInRange={} unreadable={} intradayFallback={}",
                    request.jobId(), noDaily, unreadable, fallbacks);
        }

        if (request.openFlowSince() != null) {
            addOpenFlow(request, etfs, asOf);
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
        // 요청이 운영 상태를 달라고 할 때만 읽는다. 아니면 칸 자체를 넣지 않는다.
        DailyPayload.Operations operations = request.wantsOperations() ? operations(request, since, asOf) : null;
        return new DailyPayload(request.jobId(), request.seq(), etfs, closeRates, background, truncated, operations);
    }

    /**
     * 아침 판이 요청했을 때만 ETF마다 개장 흐름을 붙인다. 읽을 구간은 요청 시작 시각부터 기준 시각까지이고, 안전 상한보다
     * 길면 기준 시각에서 그 기간만큼만 거슬러 읽는다. 한 ETF의 조회가 실패해도 나머지 응답은 보낸다(그 ETF는 칸이 없다).
     */
    private void addOpenFlow(DailyRequest request, List<DailyPayload.Etf> etfs,
            OffsetDateTime asOf) {
        OffsetDateTime requested = OffsetDateTime.parse(request.openFlowSince());
        if (requested.isAfter(asOf)) {
            throw new IllegalArgumentException("invalid daily request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }
        OffsetDateTime floor = asOf.minus(dailyProperties.openFlowMaxWindow());
        OffsetDateTime from = requested.isAfter(floor) ? requested : floor;
        int failed = 0;
        for (int i = 0; i < etfs.size(); i++) {
            DailyPayload.Etf etf = etfs.get(i);
            try {
                etfs.set(i, etf.withOpenFlow(openFlow.calculate(etf.symbol(), from, asOf)));
            } catch (RuntimeException e) {
                failed++;
            }
        }
        if (failed > 0) {
            // 종목코드는 보유 종목이 드러나므로 건수만 남긴다.
            log.warn("open flow unavailable jobId={} seq={} etfs={}", request.jobId(), request.seq(), failed);
        }
    }

    /** 대체값은 부가 정보라 시세 조회가 실패해도 그 ETF는 일봉 없음 그대로 보낸다. */
    private DailyPayload.Etf withFallback(DailyPayload.Etf without, OffsetDateTime since, OffsetDateTime asOf) {
        try {
            return dayRateFallback.apply(without, since, asOf);
        } catch (RuntimeException e) {
            log.warn("intraday fallback unavailable error={}", e.getClass().getSimpleName());
            return without;
        }
    }

    /** 운영 상태는 부가 정보라 조회가 실패해도 나머지 응답은 보낸다. 종목코드·외부 오류 원문이 새지 않게 예외 종류만 남긴다. */
    private DailyPayload.Operations operations(DailyRequest request, OffsetDateTime since,
            OffsetDateTime asOf) {
        try {
            return operationsReader.read(since, asOf);
        } catch (RuntimeException e) {
            log.warn("operations unavailable jobId={} seq={} error={}", request.jobId(), request.seq(),
                    e.getClass().getSimpleName());
            return null;
        }
    }

    private ContextPayload.EtfInfo withMemberChanges(ContextPayload.EtfInfo info, String symbol, OffsetDateTime since,
            OffsetDateTime asOf) {
        try {
            return memberChanges.changes(symbol, since, asOf).map(c -> info.withMemberChanges(wire(c))).orElse(info);
        } catch (RuntimeException e) {
            log.warn("member changes unavailable error={}", e.getClass().getSimpleName());
            return info;
        }
    }

    private static ContextPayload.MemberChanges wire(MemberChanges changes) {
        return new ContextPayload.MemberChanges(changes.since().toString(), changes.asOf().toString(),
                changes.removed().stream()
                        .map(m -> new ContextPayload.ChangedMember(m.market(), m.symbol(), m.name(), m.rank())).toList(),
                changes.added().stream()
                        .map(m -> new ContextPayload.ChangedMember(m.market(), m.symbol(), m.name(), m.rank())).toList(),
                changes.moved().stream().map(m -> new ContextPayload.MovedMember(m.market(), m.symbol(), m.name(),
                        m.rankBefore(), m.rankAfter())).toList());
    }

    /** record가 null이면 일봉 값을 비우고 missingReason을 싣는다. 미국 종목과 ETF 정보는 일봉과 관계없이 싣는다. */
    private DailyPayload.Etf etf(String symbol, DailyPriceReader.Record record,
            BackgroundRepository.StoredResponse stored, String missingReason, int maxTickers, OffsetDateTime since,
            OffsetDateTime asOf) {
        List<String> allTickers = holdings.topTickers(symbol);
        List<String> tickers = allTickers.stream().limit(maxTickers).toList();
        boolean tickersTruncated = allTickers.size() > tickers.size();
        ContextPayload.EtfInfo info =
                holdings.etfInfo(symbol).map(i -> MarketBackground.etfInfo(i, maxTickers)).orElse(null);
        if (info != null) {
            // 판 범위 안에 반영된 상위 구성 변경이 있을 때만 싣는다. 변경 조회가 실패해도 나머지 응답은 보낸다.
            info = withMemberChanges(info, symbol, since, asOf);
        }
        if (record == null) {
            return DailyPayload.Etf.withoutDaily(symbol, missingReason, tickers, tickersTruncated, info);
        }
        return new DailyPayload.Etf(symbol, record.date(), record.close(), record.prevClose(), record.rate(),
                EventFactory.formatKst(stored.fetchedAt()), tickers, tickersTruncated, info);
    }
}

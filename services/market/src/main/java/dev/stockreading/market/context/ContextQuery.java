package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Component;

import dev.stockreading.market.event.ContextPayload;
import dev.stockreading.market.constituent.EtfInfo;
import dev.stockreading.market.constituent.HoldingsPort;
import dev.stockreading.market.insight.OverseasCloseRateQuery;

/**
 * 변동 종목 한 개의 배경 자료, 담긴 미국 종목 티커, 티커별 직전 정규장 등락률을 답한다.
 *
 * <p>상태형 자료라 기준 시각(asOf) 뒤에 받은 자료는 쓰지 않는다. 받은 시각 하한은 요청 since와 기준 시각에서
 * 안전 상한 기간을 뺀 시각 중 늦은 쪽이다. 저장 상태를 바꾸지 않는 조회라 같은 요청을 다시 받아도
 * 같은 내용으로 다시 답할 뿐 부작용이 없다(브리핑은 연결 실패·시간 초과 때 같은 요청을 다시 부른다).
 */
@Component
class ContextQuery {

    private final MarketBackground marketBackground;
    private final OverseasCloseRateQuery closeRateQuery;
    private final HoldingsPort holdings;
    private final ContextProperties properties;

    ContextQuery(BackgroundRepository repository, WorldIndexRates worldIndexRates,
            OverseasCloseRateQuery closeRateQuery, HoldingsPort holdings, ContextProperties properties) {
        this.marketBackground = new MarketBackground(repository, worldIndexRates, properties.worldIndexPrevLookback());
        this.closeRateQuery = closeRateQuery;
        this.holdings = holdings;
        this.properties = properties;
    }

    /** 내부 조회 API 가 부른다. 같은 요청에는 같은 내용을 돌려준다. */
    ContextPayload answer(ContextRequest request) {
        OffsetDateTime asOf = OffsetDateTime.parse(request.asOf());
        OffsetDateTime requestedSince = OffsetDateTime.parse(request.since());
        if (request.maxResponseBytes() < 1 || request.maxTickers() < 1 || requestedSince.isAfter(asOf)) {
            // 잘못된 요청은 답하지 않는다. 내부 조회 API 는 먼저 400 으로 거른다.
            throw new IllegalArgumentException("invalid context request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }
        OffsetDateTime floor = asOf.minus(properties.maxAge());
        OffsetDateTime since = requestedSince.isAfter(floor) ? requestedSince : floor;
        int maxBytes = Math.min(request.maxResponseBytes(), properties.maxResponseBytes());
        int maxTickers = Math.min(request.maxTickers(), properties.maxTickers());

        List<String> allTickers = holdings.topTickers(request.symbol());
        List<String> tickers = allTickers.stream().limit(maxTickers).toList();
        List<ContextPayload.CloseRate> closeRates = new ArrayList<>();
        for (String ticker : tickers) {
            closeRateQuery.find(ticker, asOf, since)
                    .ifPresent(rate -> closeRates.add(MarketBackground.closeRate(ticker, rate)));
        }

        List<ContextPayload.Background> background =
                new ArrayList<>(marketBackground.market(properties.worldIndexContinents(), since, asOf, maxBytes));
        background.add(marketBackground.single(BackgroundKind.DAILY_PRICE, request.symbol(), since, asOf, maxBytes));

        Optional<EtfInfo> etf = holdings.etfInfo(request.symbol());
        return new ContextPayload(request.jobId(), request.seq(), request.symbol(), tickers, closeRates, background,
                allTickers.size() > tickers.size(),
                etf.map(info -> MarketBackground.etfInfo(info, maxTickers)).orElse(null),
                nameOf(request.symbol(), etf));
    }

    /**
     * ETF 표의 이름을 먼저 보고, 없거나 비었으면 주식 표에서 찾는다. 둘 다 없거나 비면 null이라 칸이 빠지고,
     * 이름은 앞뒤 공백을 걷는다.
     */
    private String nameOf(String symbol, Optional<EtfInfo> etf) {
        return etf.map(EtfInfo::name).filter(name -> name != null && !name.isBlank())
                .or(() -> holdings.stockName(symbol))
                .filter(name -> !name.isBlank()).map(String::strip).orElse(null);
    }
}

package dev.stockreading.collector.holdings;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.event.HoldingsEnvelope.HoldingsPayload;
import dev.stockreading.collector.event.HoldingsEnvelope.Membership;

/**
 * market.holdings 로 받은 가장 최근의 보유 종목 목록. 설정 값으로 대신하지 않으므로 목록을 받기 전에는 비어 있다.
 * 이벤트는 그때의 전체 목록이라 같은 이벤트를 다시 받아도 결과가 같다.
 */
@Component
class ReceivedHoldings implements HoldingsPort {

    private static final Logger log = LoggerFactory.getLogger(ReceivedHoldings.class);

    // 관심 ETF는 관계가 HELD 또는 WATCH인 국내 ETF다. 구성 종목으로만 아는 NONE 종목은 조회 대상이 아니다.
    private static final Set<String> TRACKED = Set.of("HELD", "WATCH");

    private volatile List<String> allTickers = List.of();
    private volatile Map<String, List<String>> etfsByTicker = Map.of();
    private volatile boolean received;

    void update(HoldingsPayload payload) {
        List<String> trackedEtfs = payload.instruments().stream()
                .filter(i -> "ETF".equals(i.kind()) && "KR".equals(i.market()) && TRACKED.contains(i.relation()))
                .map(dev.stockreading.collector.event.HoldingsEnvelope.Instrument::symbol)
                .sorted()
                .toList();
        Set<String> tracked = Set.copyOf(trackedEtfs);
        LinkedHashSet<String> tickers = new LinkedHashSet<>();
        payload.memberships().stream()
                .filter(m -> "KR".equals(m.etfMarket()) && tracked.contains(m.etfSymbol())
                        && "US".equals(m.memberMarket()))
                .sorted(Comparator.comparing((Membership m) -> m.etfSymbol())
                        .thenComparing(m -> m.rank() == null ? Integer.MAX_VALUE : m.rank())
                        .thenComparing(m -> m.memberSymbol()))
                .forEach(m -> tickers.add(m.memberSymbol()));
        Map<String, Set<String>> etfs = new TreeMap<>();
        payload.memberships().stream()
                .filter(m -> "KR".equals(m.etfMarket()) && tracked.contains(m.etfSymbol())
                        && "US".equals(m.memberMarket()))
                .forEach(m -> etfs.computeIfAbsent(m.memberSymbol(), k -> new TreeSet<>()).add(m.etfSymbol()));
        Map<String, List<String>> etfsByTicker = new TreeMap<>();
        etfs.forEach((ticker, set) -> etfsByTicker.put(ticker, List.copyOf(set)));
        this.etfsByTicker = Map.copyOf(etfsByTicker);
        this.allTickers = List.copyOf(tickers);
        this.received = true;
        // 코드와 티커는 보유 종목이 드러나므로 개수만 남긴다.
        log.info("보유 종목 목록을 받았습니다: 관심 ETF {}개, 고유 미국 티커 {}개", trackedEtfs.size(), tickers.size());
    }

    @Override
    public List<String> allTickers() {
        if (!received) {
            log.warn("보유 종목 목록을 아직 받지 못했습니다(market.holdings). 빈 목록으로 처리합니다");
        }
        return allTickers;
    }

    @Override
    public List<String> etfsHolding(String ticker) {
        return etfsByTicker.getOrDefault(ticker, List.of());
    }
}

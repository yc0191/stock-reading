package dev.stockreading.collector.holdings;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * 수집 대상을 DB에서 읽기 전용으로 읽는다. 사용 중인 멤버의 포트폴리오를 합친 뷰(personal.collect_target)와
 * ETF가 담은 종목(market.etf_stock_map)을 시작할 때 한 번 읽고 메모리에 둔다. 바뀐 값은 재시작해야 반영된다.
 * 읽지 못하면 시작을 멈춘다. 빈 목록으로 시작하면 수집이 조용히 비기 때문이다.
 */
@Component
class DbCollectTargets implements HoldingsPort, QuoteSymbolsPort {

    private static final Logger log = LoggerFactory.getLogger(DbCollectTargets.class);

    private final List<String> quoteSymbols;
    private final List<String> usTickers;
    private final Map<String, List<String>> etfsByTicker;

    DbCollectTargets(JdbcClient jdbc) {
        this.quoteSymbols = jdbc.sql("SELECT symbol FROM personal.collect_target WHERE purpose = 'KR_QUOTE' ORDER BY symbol")
                .query(String.class).list();
        this.usTickers = jdbc.sql("SELECT symbol FROM personal.collect_target WHERE purpose = 'US_DAILY' ORDER BY symbol")
                .query(String.class).list();
        // 미국 티커를 구성 종목으로 담은 ETF 중 포트폴리오에 있는 국내 ETF만 찾는다.
        Map<String, TreeSet<String>> etfs = new TreeMap<>();
        jdbc.sql("""
                        SELECT s.symbol AS ticker, e.symbol AS etf
                        FROM market.etf_stock_map m
                        JOIN market.etf e ON e.id = m.etf_id
                        JOIN market.stock s ON s.id = m.stock_id
                        WHERE e.market = 'KR' AND s.market = 'US'
                          AND e.symbol IN (SELECT symbol FROM personal.collect_target
                                           WHERE purpose = 'KR_QUOTE' AND kind = 'ETF')
                        """)
                .query((rs, rowNum) -> {
                    etfs.computeIfAbsent(rs.getString("ticker"), k -> new TreeSet<>()).add(rs.getString("etf"));
                    return null;
                }).list();
        Map<String, List<String>> copy = new TreeMap<>();
        etfs.forEach((ticker, set) -> copy.put(ticker, List.copyOf(set)));
        this.etfsByTicker = Map.copyOf(copy);
        // 코드와 티커는 보유 종목이 드러나므로 개수만 남긴다.
        log.info("수집 대상을 DB에서 읽었습니다: 국내 시세 {}개, 미국 일봉 {}개", quoteSymbols.size(), usTickers.size());
        if (quoteSymbols.isEmpty() && usTickers.isEmpty()) {
            log.warn("수집 대상이 비어 있습니다(사용 중인 멤버의 포트폴리오가 없음). 값을 넣은 뒤 재시작해야 합니다");
        }
    }

    @Override
    public List<String> symbols() {
        return quoteSymbols;
    }

    @Override
    public List<String> targetSymbols() {
        return Stream.concat(quoteSymbols.stream(), usTickers.stream()).distinct().sorted().toList();
    }

    @Override
    public List<String> allTickers() {
        return usTickers;
    }

    @Override
    public List<String> etfsHolding(String ticker) {
        return etfsByTicker.getOrDefault(ticker, List.of());
    }
}

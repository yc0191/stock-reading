package dev.stockreading.collector.holdings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** 설정(collector.holdings.us-by-etf)에서 읽은 보유 종목 목록. 시작할 때 한 번 검증해 두고 바뀌지 않는다. */
@Component
class ConfiguredHoldings implements HoldingsPort {

    private static final Logger log = LoggerFactory.getLogger(ConfiguredHoldings.class);

    private final Map<String, List<String>> tickersByEtf = new LinkedHashMap<>();
    private final List<String> allTickers;

    ConfiguredHoldings(HoldingsProperties properties) {
        // 설정 오타로 일부 종목이 조용히 빠지지 않도록 시작 단계에서 실패시킨다.
        for (EtfHoldings holdings : EtfHoldings.parseValidated(properties.usByEtf())) {
            tickersByEtf.put(holdings.etfCode(), holdings.tickers());
        }
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        tickersByEtf.values().forEach(unique::addAll);
        this.allTickers = List.copyOf(unique);
        // 코드와 티커는 보유 종목이 드러나므로 개수만 남긴다.
        log.info("보유 종목 설정을 읽었습니다: ETF {}개, 고유 티커 {}개", tickersByEtf.size(), allTickers.size());
    }

    @Override
    public List<String> etfCodes() {
        return new ArrayList<>(tickersByEtf.keySet());
    }

    @Override
    public List<String> topTickers(String etfCode) {
        return tickersByEtf.getOrDefault(etfCode, List.of());
    }

    @Override
    public List<String> allTickers() {
        return allTickers;
    }
}

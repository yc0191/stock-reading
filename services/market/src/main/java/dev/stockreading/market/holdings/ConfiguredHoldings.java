package dev.stockreading.market.holdings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

/**
 * 저장소 밖 보유 종목 파일(market.holdings.file)을 시작할 때 한 번 읽고 검증해 목표 상태(plan)로 들고 있다.
 * 파일의 미국 구성 종목이 수집기가 읽는 ETF_US_HOLDINGS와 다르면 시작을 실패시킨다. 두 값이 어긋나면 수집기가 모으는
 * 종목과 브리핑이 쓰는 종목이 달라져 일부 종목의 자료가 오류 없이 빠지기 때문이다.
 */
@Component
class ConfiguredHoldings {

    private static final Logger log = LoggerFactory.getLogger(ConfiguredHoldings.class);

    private final HoldingsPlan plan;

    ConfiguredHoldings(HoldingsProperties properties, ObjectMapper mapper) {
        String json = read(properties.file());
        this.plan = HoldingsFileParser.parse(json, mapper);
        checkAgainstCollectorSetting(plan, properties.usByEtf());
        // 코드와 티커는 보유 종목이 드러나므로 개수만 남긴다.
        log.info("보유 종목 파일을 읽었습니다: 종목 {}개, 구성 관계 {}개", plan.instruments().size(), plan.memberships().size());
    }

    HoldingsPlan plan() {
        return plan;
    }

    // 파일이 없거나 못 읽는 것도 시작 실패다. 경로는 메시지에 넣지 않는다.
    private static String read(String file) {
        if (file == null || file.isBlank()) {
            return "";
        }
        try {
            return Files.readString(Path.of(file.strip()), StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("보유 종목 파일을 읽을 수 없습니다");
        }
    }

    /** 파일의 ETF별 미국 구성 종목 집합이 ETF_US_HOLDINGS의 ETF별 티커 집합과 같아야 한다(순서는 보지 않는다). */
    static void checkAgainstCollectorSetting(HoldingsPlan plan, String usByEtf) {
        Map<String, Set<String>> fromEnv = new LinkedHashMap<>();
        for (EtfHoldings holdings : EtfHoldings.parseValidated(usByEtf)) {
            fromEnv.put(holdings.etfCode(), new HashSet<>(holdings.tickers()));
        }
        Map<String, Set<String>> fromFile = new HashMap<>();
        for (HoldingsPlan.Membership m : plan.memberships()) {
            if (m.memberMarket().equals("US") && m.etfMarket().equals("KR")) {
                fromFile.computeIfAbsent(m.etfSymbol(), k -> new HashSet<>()).add(m.memberSymbol());
            }
        }
        if (!fromEnv.equals(fromFile)) {
            Set<String> differing = new HashSet<>(fromEnv.keySet());
            differing.addAll(fromFile.keySet());
            differing.removeIf(etf -> fromEnv.getOrDefault(etf, Set.of()).equals(fromFile.getOrDefault(etf, Set.of())));
            throw new IllegalStateException("보유 종목 파일의 미국 구성 종목이 ETF_US_HOLDINGS와 어긋납니다(어긋난 ETF "
                    + differing.size() + "개)");
        }
    }
}

package dev.stockreading.news.holdings;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import dev.stockreading.news.event.HoldingsEnvelope;

/**
 * market.holdings 로 받은 가장 최근의 ETF 구성 관계. 이벤트는 그때의 전체 목록이라 같은 이벤트를 다시 받아도 결과가 같다.
 * 목록을 받기 전에는 구성 종목이 없는 것으로 보므로, ETF 기사 조회는 그 ETF에 직접 태그된 기사만 돌려준다.
 */
@Component
public class ReceivedHoldings {

    private static final Logger log = LoggerFactory.getLogger(ReceivedHoldings.class);

    // 기사 응답의 종목은 국내 종목코드라 국내 ETF의 구성 종목만 모은다.
    private static final String ETF_MARKET = "KR";

    private volatile Map<String, Set<String>> membersByEtf = Map.of();

    void update(HoldingsEnvelope.HoldingsPayload payload) {
        Map<String, Set<String>> received = new HashMap<>();
        for (HoldingsEnvelope.Membership m : payload.memberships()) {
            if (ETF_MARKET.equals(m.etfMarket()) && m.etfSymbol() != null && m.memberSymbol() != null) {
                received.computeIfAbsent(m.etfSymbol(), k -> new HashSet<>()).add(m.memberSymbol());
            }
        }
        this.membersByEtf = Map.copyOf(received);
        // 종목코드는 보유 종목이 드러나므로 개수만 남긴다.
        log.info("구성 관계를 받았습니다: ETF {}개", received.size());
    }

    /** 그 ETF가 담은 구성 종목(국내 코드와 미국 티커). ETF가 아니거나 아직 목록을 받지 못했으면 빈 집합이다. */
    public Set<String> membersOf(String etfSymbol) {
        return membersByEtf.getOrDefault(etfSymbol, Set.of());
    }
}

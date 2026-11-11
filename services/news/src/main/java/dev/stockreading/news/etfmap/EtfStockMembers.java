package dev.stockreading.news.etfmap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * ETF가 담은 구성 종목을 DB(market.etf_stock_map)에서 읽기 전용으로 읽는다. 시작할 때 한 번 읽고 메모리에 두며, 바뀐 값은
 * 재시작해야 반영된다. ETF 기사 조회가 그 ETF 자체 기사에 더해 구성 종목 기사까지 꺼내는 데 쓴다.
 * 읽지 못하면 시작을 멈춘다. 빈 목록으로 시작하면 ETF 기사가 조용히 좁아지기 때문이다.
 */
@Component
public class EtfStockMembers {

    private static final Logger log = LoggerFactory.getLogger(EtfStockMembers.class);

    // 기사 응답의 종목은 국내 종목코드라 국내 ETF의 구성 종목만 모은다.
    private static final String ETF_MARKET = "KR";

    private final Map<String, Set<String>> membersByEtf;

    EtfStockMembers(JdbcClient jdbc) {
        Map<String, Set<String>> read = new HashMap<>();
        jdbc.sql("""
                        SELECT e.symbol AS etf, s.symbol AS member
                        FROM market.etf_stock_map m
                        JOIN market.etf e ON e.id = m.etf_id
                        JOIN market.stock s ON s.id = m.stock_id
                        WHERE e.market = :market
                        """)
                .param("market", ETF_MARKET)
                .query((rs, rowNum) -> {
                    read.computeIfAbsent(rs.getString("etf"), k -> new HashSet<>()).add(rs.getString("member"));
                    return null;
                }).list();
        this.membersByEtf = Map.copyOf(read);
        // 종목코드는 관심 종목이 드러나므로 개수만 남긴다.
        log.info("ETF 구성 종목을 DB에서 읽었습니다: ETF {}개", read.size());
    }

    /** 그 ETF가 담은 구성 종목(국내 코드와 미국 티커). ETF가 아니거나 담은 종목이 없으면 빈 집합이다. */
    public Set<String> membersOf(String etfSymbol) {
        return membersByEtf.getOrDefault(etfSymbol, Set.of());
    }
}

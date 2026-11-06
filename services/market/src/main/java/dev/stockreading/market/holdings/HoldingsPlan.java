package dev.stockreading.market.holdings;

import java.math.BigDecimal;
import java.util.List;

/**
 * 보유 종목 파일을 읽어 만든, 테이블에 맞출 목표 상태. 종목(instrument)은 한 번만 담고,
 * ETF에 담긴 관계(membership)는 ETF마다 따로 담는다.
 */
record HoldingsPlan(List<Instrument> instruments, List<Membership> memberships) {

    HoldingsPlan {
        instruments = List.copyOf(instruments);
        memberships = List.copyOf(memberships);
    }

    static HoldingsPlan empty() {
        return new HoldingsPlan(List.of(), List.of());
    }

    record Instrument(String kind, String market, String symbol, String name, String tracks, String sector,
            String relation) {
    }

    record Membership(String etfMarket, String etfSymbol, String memberMarket, String memberSymbol,
            BigDecimal weight, int rank) {
    }
}

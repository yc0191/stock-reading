package dev.stockreading.market.holdings;

import java.math.BigDecimal;
import java.util.List;

/** ETF 한 종목의 이름, 추종 대상, 업종과 구성 종목(비중 순위 순). 값이 없는 칸은 null이다. */
public record EtfInfo(String name, String tracks, String sector, List<Member> members) {

    public EtfInfo {
        members = List.copyOf(members);
    }

    /** 구성 종목 한 개. weight(%)는 모르면 null이고, rank는 비중 순위(1이 가장 큼)다. */
    public record Member(String market, String symbol, String name, BigDecimal weight, int rank) {
    }
}

package dev.stockreading.market.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * market.holdings v1의 payload. 보유 종목 테이블의 그때 전체 목록이다.
 * contentHash는 목록 내용이 같으면 같은 값이라 소비자가 바뀌었는지 비교할 수 있다.
 */
public record HoldingsPayload(String contentHash, List<Instrument> instruments, List<Membership> memberships) {

    /** name은 모르면 칸을 뺀다(null로 보내지 않는다). */
    public record Instrument(String kind, String market, String symbol, String relation,
            @JsonInclude(JsonInclude.Include.NON_NULL) String name) {
    }

    /** rank는 모르면 null이다. */
    public record Membership(String etfMarket, String etfSymbol, String memberMarket, String memberSymbol,
            Integer rank) {
    }
}

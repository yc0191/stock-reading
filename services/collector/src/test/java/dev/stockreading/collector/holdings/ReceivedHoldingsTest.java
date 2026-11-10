package dev.stockreading.collector.holdings;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.collector.event.HoldingsEnvelope.HoldingsPayload;
import dev.stockreading.collector.event.HoldingsEnvelope.Instrument;
import dev.stockreading.collector.event.HoldingsEnvelope.Membership;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. */
class ReceivedHoldingsTest {

    private static Instrument etf(String symbol, String relation) {
        return new Instrument("ETF", "KR", symbol, relation);
    }

    private static Membership member(String etf, String market, String symbol, Integer rank) {
        return new Membership("KR", etf, market, symbol, rank);
    }

    private final ReceivedHoldings holdings = new ReceivedHoldings();

    @Test
    void beforeAnyListIsReceivedThereAreNoTickersAndNothingIsTakenFromSettings() {
        assertThat(holdings.allTickers()).isEmpty();
    }

    @Test
    void tickersAreUniqueInEtfCodeOrderThenRankOrder() {
        holdings.update(new HoldingsPayload("h", List.of(etf("TEST02", "WATCH"), etf("TEST01", "HELD")), List.of(
                member("TEST02", "US", "FAKEB", 1), member("TEST02", "US", "FAKEA", 2),
                member("TEST01", "US", "FAKEA", 2), member("TEST01", "US", "FAKEC", 1),
                member("TEST01", "US", "FAKED", null))));

        // TEST01: FAKEC(1) FAKEA(2) FAKED(순위 없음) → TEST02: FAKEB(1) (FAKEA는 이미 나옴)
        assertThat(holdings.allTickers()).containsExactly("FAKEC", "FAKEA", "FAKED", "FAKEB");
    }

    @Test
    void onlyHeldOrWatchedKrEtfsAndTheirUsMembersCount() {
        holdings.update(new HoldingsPayload("h", List.of(
                etf("TEST01", "HELD"), etf("TEST02", "NONE"), new Instrument("STOCK", "KR", "TEST03", "HELD"),
                new Instrument("ETF", "US", "FAKEE", "HELD")), List.of(
                member("TEST01", "US", "FAKEA", 1), member("TEST01", "KR", "TEST09", 2),
                member("TEST02", "US", "FAKEB", 1), member("TEST03", "US", "FAKEC", 1),
                new Membership("US", "FAKEE", "US", "FAKED", 1))));

        assertThat(holdings.allTickers()).containsExactly("FAKEA");
    }

    @Test
    void eachListReplacesThePreviousOneAndTheSameListTwiceGivesTheSameResult() {
        HoldingsPayload first = new HoldingsPayload("h1", List.of(etf("TEST01", "HELD")),
                List.of(member("TEST01", "US", "FAKEA", 1)));
        HoldingsPayload second = new HoldingsPayload("h2", List.of(etf("TEST01", "HELD")),
                List.of(member("TEST01", "US", "FAKEB", 1)));

        holdings.update(first);
        holdings.update(first);
        assertThat(holdings.allTickers()).containsExactly("FAKEA");
        holdings.update(second);
        assertThat(holdings.allTickers()).containsExactly("FAKEB");
        holdings.update(new HoldingsPayload("h3", List.of(), List.of()));
        assertThat(holdings.allTickers()).isEmpty();
    }
}

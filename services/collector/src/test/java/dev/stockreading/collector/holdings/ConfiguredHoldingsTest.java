package dev.stockreading.collector.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. */
class ConfiguredHoldingsTest {

    private static HoldingsPort holdings(String spec) {
        return new ConfiguredHoldings(new HoldingsProperties(spec));
    }

    @Test
    void returnsTickersOfOneEtfInConfiguredOrder() {
        HoldingsPort port = holdings("TEST01=FAKEA,FAKEB;TEST02=FAKEB,FAKEC");

        assertThat(port.etfCodes()).containsExactly("TEST01", "TEST02");
        assertThat(port.topTickers("TEST01")).containsExactly("FAKEA", "FAKEB");
        assertThat(port.topTickers("TEST02")).containsExactly("FAKEB", "FAKEC");
    }

    @Test
    void unknownEtfCodeGivesAnEmptyListInsteadOfFailing() {
        assertThat(holdings("TEST01=FAKEA").topTickers("TEST99")).isEmpty();
        assertThat(holdings("TEST01=FAKEA").topTickers(null)).isEmpty();
    }

    @Test
    void allTickersAreUniqueAcrossEtfsInFirstSeenOrder() {
        HoldingsPort port = holdings("TEST01=FAKEA,FAKEB;TEST02=FAKEB,FAKEC;TEST03=FAKEA");

        assertThat(port.allTickers()).containsExactly("FAKEA", "FAKEB", "FAKEC");
    }

    @Test
    void sameClassShareWrittenDifferentlyInTwoEtfsIsQueriedOnce() {
        // 한 ETF 에는 ".", 다른 ETF 에는 "-" 로 적어도 같은 종목으로 한 번만 조회한다.
        HoldingsPort port = holdings("TEST01=FAKE.B,FAKEA;TEST02=FAKE-B");

        assertThat(port.allTickers()).containsExactly("FAKE-B", "FAKEA");
    }

    @Test
    void blankSettingMeansNoHoldings() {
        HoldingsPort port = holdings("");

        assertThat(port.etfCodes()).isEmpty();
        assertThat(port.allTickers()).isEmpty();
        assertThat(holdings(null).allTickers()).isEmpty();
    }

    @Test
    void malformedSettingFailsConstructionWithoutEchoingTheValue() {
        assertThatThrownBy(() -> holdings("TEST01=FAKEA;TEST02=fakeb"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2번째")
                .hasMessageNotContaining("TEST02")
                .hasMessageNotContaining("fakeb");
    }

    @Test
    void propertiesToStringHidesTheValue() {
        assertThat(new HoldingsProperties("TEST01=FAKEA").toString())
                .doesNotContain("TEST01")
                .doesNotContain("FAKEA");
    }
}

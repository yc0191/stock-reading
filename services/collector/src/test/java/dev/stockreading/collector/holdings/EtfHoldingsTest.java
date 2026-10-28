package dev.stockreading.collector.holdings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** 가짜 값(TEST 접두 ETF 코드, 가짜 티커)만 쓴다. 실제 보유 종목은 테스트에 적지 않는다. */
class EtfHoldingsTest {

    @Test
    void parsesEtfCodesWithTickersInConfiguredOrder() {
        List<EtfHoldings> holdings = EtfHoldings.parseValidated("TEST01=FAKEA,FAKEB; TEST02 = FAKEB , FAKEC ");

        assertThat(holdings).extracting(EtfHoldings::etfCode).containsExactly("TEST01", "TEST02");
        assertThat(holdings.get(0).tickers()).containsExactly("FAKEA", "FAKEB");
        assertThat(holdings.get(1).tickers()).containsExactly("FAKEB", "FAKEC");
    }

    @Test
    void acceptsClassSuffixWithDotOrDashAndDigitsInTickers() {
        List<EtfHoldings> holdings = EtfHoldings.parseValidated("TEST01=FAKE.A,FAKE-B,F4KE,A");

        assertThat(holdings.get(0).tickers()).containsExactly("FAKE.A", "FAKE-B", "F4KE", "A");
    }

    @Test
    void blankSpecGivesNoHoldingsNotAnError() {
        assertThat(EtfHoldings.parseValidated(null)).isEmpty();
        assertThat(EtfHoldings.parseValidated("")).isEmpty();
        assertThat(EtfHoldings.parseValidated("  ")).isEmpty();
    }

    @Test
    void ignoresBlankEntriesAndATrailingSemicolon() {
        List<EtfHoldings> holdings = EtfHoldings.parseValidated("  ; TEST01=FAKEA ;; TEST02=FAKEB ;  ;");

        assertThat(holdings).extracting(EtfHoldings::etfCode).containsExactly("TEST01", "TEST02");
    }

    @Test
    void dropsDuplicateTickersWithinOneEtfAndIgnoresEmptyTickerSlots() {
        List<EtfHoldings> holdings = EtfHoldings.parseValidated("TEST01=FAKEA,,FAKEB, ,FAKEA");

        assertThat(holdings.get(0).tickers()).containsExactly("FAKEA", "FAKEB");
    }

    @ParameterizedTest
    @ValueSource(strings = {"noequals", "=FAKEA", "  =FAKEA", "TEST99=", "TEST99= , ", "TEST99"})
    void rejectsStructureErrorsWithoutEchoingTheSpec(String entry) {
        String spec = "TEST01=FAKEA;" + entry;

        assertThatThrownBy(() -> EtfHoldings.parseValidated(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2번째")
                .hasMessageContaining("구조 오류")
                .hasMessageNotContaining("TEST99")
                .hasMessageNotContaining("noequals")
                .hasMessageNotContaining("FAKEA");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TEST9", "test99", "TEST999", "TEST-9", "TEST 9"})
    void rejectsMalformedEtfCodesWithoutEchoingTheSpec(String etfCode) {
        String spec = "TEST01=FAKEA;" + etfCode + "=FAKEB";

        assertThatThrownBy(() -> EtfHoldings.parseValidated(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2번째")
                .hasMessageContaining("ETF 코드 형식 오류")
                .hasMessageNotContaining(etfCode)
                .hasMessageNotContaining("FAKEB");
    }

    @ParameterizedTest
    @ValueSource(strings = {"fakea", "FAKEAAA", "FAKE.ABC", "FAKE_A", "FAKE/A", ".A", "FAKE-", "FAKEA,fakeb"})
    void rejectsMalformedTickersWithoutEchoingTheSpec(String tickers) {
        String spec = "TEST01=FAKEA;TEST99=" + tickers;

        assertThatThrownBy(() -> EtfHoldings.parseValidated(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2번째")
                .hasMessageContaining("티커 형식 오류")
                .hasMessageNotContaining("TEST99")
                .hasMessageNotContaining(tickers);
    }

    @Test
    void rejectsADuplicateEtfCodeWithoutEchoingTheSpec() {
        assertThatThrownBy(() -> EtfHoldings.parseValidated("TEST01=FAKEA;TEST02=FAKEB;TEST01=FAKEC"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("3번째")
                .hasMessageContaining("ETF 코드 중복 오류")
                .hasMessageNotContaining("TEST01")
                .hasMessageNotContaining("FAKEC");
    }

    @Test
    void positionCountsBlankEntriesAndStopsAtTheFirstError() {
        // 순번은 공백 항목도 센다: 1 정상, 2 공백, 3 정상, 4 구조 오류
        assertThatThrownBy(() -> EtfHoldings.parseValidated("TEST01=FAKEA; ;TEST02=FAKEB;bad"))
                .hasMessageContaining("4번째")
                .hasMessageContaining("구조 오류");
        // 1 정상, 2 구조 오류, 3 티커 오류: 첫 오류에서 멈춘다.
        assertThatThrownBy(() -> EtfHoldings.parseValidated("TEST01=FAKEA;noequals;TEST02=fakeb"))
                .hasMessageContaining("2번째")
                .hasMessageContaining("구조 오류");
    }

    @Test
    void tickersAreUnmodifiable() {
        List<String> tickers = EtfHoldings.parseValidated("TEST01=FAKEA").get(0).tickers();

        assertThatThrownBy(() -> tickers.add("FAKEB")).isInstanceOf(UnsupportedOperationException.class);
    }
}

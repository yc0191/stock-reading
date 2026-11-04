package dev.stockreading.collector.marketdata;

import static dev.stockreading.collector.marketdata.PriceFieldCheck.Check.CHANGE;
import static dev.stockreading.collector.marketdata.PriceFieldCheck.Check.HIGH_LOW;
import static dev.stockreading.collector.marketdata.PriceFieldCheck.Check.RATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/** 값은 모두 가짜다. 전일 종가 10000, 하락 100(-1.00%)이면 맞는 현재가는 9900 이다. */
class PriceFieldCheckTest {

    private final PriceFieldCheck check = PriceChecks.fake();

    private static String normal(String price) {
        return PriceChecks.body(price, "10000", "100", "D", "-1.00", "10100", "9800");
    }

    @Test
    void consistentResponsePasses() {
        assertThat(check.failedChecks(9900, normal("9900"))).isEmpty();
    }

    @Test
    void brokenPriceAloneFailsEveryCheck() {
        assertThat(check.failedChecks(12345, normal("12345"))).containsExactly(CHANGE, RATE, HIGH_LOW);
    }

    @Test
    void brokenPriceInsideTheDayRangeIsStillCaughtByChangeAndRate() {
        // 그날 고가·저가 안의 그럴듯한 값도 전일대비·등락률과는 어긋난다.
        assertThat(check.failedChecks(10050, normal("10050"))).containsExactly(CHANGE, RATE);
    }

    @Test
    void dateShapedOrTinyPriceIsDropped() {
        // 날짜 모양의 8자리 값이나 두세 자리 값이 현재가 칸에 들어온 모양을 본뜬다.
        assertThat(check.failedChecks(20261104, normal("20261104"))).containsExactly(CHANGE, RATE, HIGH_LOW);
        assertThat(check.failedChecks(85, normal("85"))).containsExactly(CHANGE, RATE, HIGH_LOW);
        assertThat(check.failedChecks(412, normal("412"))).containsExactly(CHANGE, RATE, HIGH_LOW);
    }

    @Test
    void anotherSymbolsPriceIsDroppedWhetherOrNotItFallsInTheDayRange() {
        // TEST_OTHER 종목의 그럴듯한 가격이 섞여 온 모양이다. 고가·저가 안에 들어와도 전일대비·등락률로 거른다.
        assertThat(check.failedChecks(23450, normal("23450"))).containsExactly(CHANGE, RATE, HIGH_LOW);
        assertThat(check.failedChecks(9960, normal("9960"))).containsExactly(CHANGE, RATE);
    }

    @Test
    void brokenPriceWithABrokenHighIsJudgedByTheRemainingChecks() {
        // 현재가와 고가가 함께 엉뚱하게 와 고저가 검사는 통과하지만, 남은 전일대비·등락률 둘이 어긋나 버린다.
        String body = PriceChecks.body("23450", "10000", "100", "D", "-1.00", "23450", "9800");

        assertThat(check.failedChecks(23450, body)).containsExactly(CHANGE, RATE);
    }

    @Test
    void knownGapOfRuleATieOfOneFailAndOnePassIsKept() {
        // 규칙 A의 알려진 빈틈(동률은 통과): 전일대비 칸이 비어 판단한 검사가 등락률(실패)·고저가(통과) 둘뿐이면
        // 1대1이라 과반이 아니어서 깨진 값이어도 시세로 보낸다. 바꾸려면 규칙부터 다시 정한다.
        String body = PriceChecks.body("10050", "10000", "", "D", "-1.00", "10100", "9800");

        assertThat(check.failedChecks(10050, body)).isEmpty();
    }

    @Test
    void midpointTradeWithinToleranceIsKept() {
        // 호가 사이 가운데 체결은 5원 단위가 아니고 전일대비·등락률은 5원 단위 가격으로 온다.
        assertThat(check.failedChecks(9902, normal("9902"))).isEmpty();
    }

    @Test
    void oneBrokenComparisonFieldDoesNotDropAGoodPrice() {
        String brokenHigh = PriceChecks.body("9900", "10000", "100", "D", "-1.00", "1", "9800");
        String brokenRate = PriceChecks.body("9900", "10000", "100", "D", "55.55", "10100", "9800");

        assertThat(check.failedChecks(9900, brokenHigh)).isEmpty();
        assertThat(check.failedChecks(9900, brokenRate)).isEmpty();
    }

    @Test
    void anotherBaseFieldRescuesABrokenPreviousClose() {
        String body = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{\"now_prc\":\"9900\",\""
                + PriceChecks.BASE + "\":\"77777\",\"" + PriceChecks.BASE_ALT + "\":\"10000\",\"" + PriceChecks.CHANGE
                + "\":\"100\",\"" + PriceChecks.SIGN + "\":\"D\",\"" + PriceChecks.RATE + "\":\"-1.00\"}}";

        assertThat(check.failedChecks(9900, body)).isEmpty();
    }

    @Test
    void emptyOrNonNumericComparisonFieldsAreNotJudged() {
        // 전일대비·등락률 칸이 비거나 숫자가 아니면 고저가 하나로 판단한다.
        String onlyHighLowLeft = PriceChecks.body("12345", "10000", "", "D", "abc", "10100", "9800");
        String everythingEmpty = PriceChecks.body("12345", "", "", "", " ", "", "");

        assertThat(check.failedChecks(12345, onlyHighLowLeft)).containsExactly(HIGH_LOW);
        assertThat(check.failedChecks(12345, everythingEmpty)).isEmpty();
        assertThat(check.failedChecks(12345, "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER
                + ",\"dataBody\":{\"now_prc\":\"12345\"}}")).isEmpty();
    }

    @Test
    void numbersWithLeadingSpacesAreRead() {
        String spaced = PriceChecks.body("9900", " 10000", "  100", "D", "   -1.00", " 10100", " 9800");

        assertThat(check.failedChecks(9900, spaced)).isEmpty();
        assertThat(check.failedChecks(12345, spaced.replace("\"9900\"", "\"12345\""))).hasSize(3);
    }

    @Test
    void unknownSignCodeLeavesTheChangeCheckUnjudged() {
        String body = PriceChecks.body("12345", "10000", "100", "?", "-1.00", null, null);

        // 판단한 검사는 등락률 하나뿐이고 그것이 어긋나므로 버린다.
        assertThat(check.failedChecks(12345, body)).containsExactly(RATE);
    }

    @Test
    void toleranceBoundaryEqualPassesAndJustOutsideFails() {
        // 보합 10000, 허용 오차 0.3% = 30. 등락률은 반올림 폭 0.5 를 더 허용한다.
        String flat = PriceChecks.body("PRICE", "10000", "0", "F", "0.00", null, null);

        assertThat(check.failedChecks(10029, flat.replace("PRICE", "10029"))).isEmpty();
        assertThat(check.failedChecks(10030, flat.replace("PRICE", "10030"))).isEmpty();
        assertThat(check.failedChecks(10031, flat.replace("PRICE", "10031"))).containsExactly(CHANGE, RATE);
        assertThat(check.failedChecks(9970, flat.replace("PRICE", "9970"))).isEmpty();
        assertThat(check.failedChecks(9969, flat.replace("PRICE", "9969"))).containsExactly(CHANGE, RATE);
    }

    @Test
    void highLowBoundaryUsesTheSameTolerance() {
        String range = PriceChecks.body("PRICE", null, null, null, null, "10000", "9000");

        assertThat(check.failedChecks(10030, range.replace("PRICE", "10030"))).isEmpty();
        assertThat(check.failedChecks(10031, range.replace("PRICE", "10031"))).containsExactly(HIGH_LOW);
        assertThat(check.failedChecks(8973, range.replace("PRICE", "8973"))).isEmpty();
        assertThat(check.failedChecks(8972, range.replace("PRICE", "8972"))).containsExactly(HIGH_LOW);
    }

    @Test
    void zeroToleranceDropsAMidpointTrade() {
        var strict = new PriceFieldCheck(PriceChecks.properties(true, "0"), JsonMapper.builder().build());

        assertThat(strict.failedChecks(9902, normal("9902"))).containsExactly(CHANGE, RATE);
    }

    @Test
    void disabledCheckPassesEverything() {
        var off = new PriceFieldCheck(PriceChecks.properties(false, "0.003"), JsonMapper.builder().build());

        assertThat(off.failedChecks(12345, normal("12345"))).isEmpty();
    }

    @Test
    void noConfiguredFieldsMeansNothingToJudge() {
        var unnamed = new PriceFieldCheck(new PriceCheckProperties(true, null, List.of(""), "", "", null, null, null,
                null, null, null), JsonMapper.builder().build());

        assertThat(unnamed.failedChecks(12345, normal("12345"))).isEmpty();
    }

    @Test
    void exceptionTypeNamesTheFailedChecks() {
        assertThat(PriceFieldCheck.exceptionType(List.of(CHANGE, RATE))).isEqualTo("FieldMismatch:CHANGE+RATE");
    }

    @Test
    void toleranceDefaultsAndRejectsOutOfRangeValues() {
        assertThat(new PriceCheckProperties(true, null, null, null, null, null, null, null, null, null, null).tolerance())
                .isEqualByComparingTo("0.003");
        assertThatThrownBy(() -> PriceChecks.properties(true, "-0.001")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PriceChecks.properties(true, "1")).isInstanceOf(IllegalArgumentException.class);
        assertThat(PriceChecks.properties(true, "0").tolerance()).isEqualByComparingTo(BigDecimal.ZERO);
    }
}

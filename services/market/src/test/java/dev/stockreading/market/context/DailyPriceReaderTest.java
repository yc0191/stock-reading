package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/** 국내 일봉 원문에서 하루 등락 레코드를 고르는 규칙. 값은 모두 가짜다. */
class DailyPriceReaderTest {

    private final DailyPriceReader reader = new DailyPriceReader(JsonMapper.builder().build());

    @Test
    void picksTheMostRecentDateWhateverItsPositionAndCopiesRawValuesWithoutWhitespace() {
        String response = body(
                record("20261019", "      10000", "       9900", "   1.01"),
                record("20261020", "       9850", "      10000", "  -1.50"),
                record("20261016", "       9900", "       9800", "   1.02"));

        assertThat(reader.latest(response)).contains(
                new DailyPriceReader.Record("20261020", "9850", "10000", "-1.50"));
    }

    @Test
    void doesNotRecomputeTheRateEvenWhenItDisagreesWithTheCloses() {
        // 원문 등락률은 제공자 반올림을 따른다. 계산값(0.13)과 달라도 원문(0.12)을 옮긴다.
        String response = body(record("20261020", "10013", "10000", " 0.12"));

        assertThat(reader.latest(response).orElseThrow().rate()).isEqualTo("0.12");
    }

    @Test
    void recordWithABlankValueIsSkippedAndTheNextMostRecentOneIsUsed() {
        String response = body(
                record("20261020", "9850", "10000", "   "),
                record("20261019", "10000", "9900", "1.01"));

        assertThat(reader.latest(response).orElseThrow().date()).isEqualTo("20261019");
    }

    @Test
    void responseThatIsNotJsonOrHasNoRecordsGivesNothing() {
        assertThat(reader.latest("not json")).isEmpty();
        assertThat(reader.latest("{\"dataBody\": {\"out2\": []}}")).isEmpty();
        assertThat(reader.latest("{\"dataBody\": {}}")).isEmpty();
    }

    static String body(String... records) {
        return "{\"dataBody\": {\"out2\": [" + String.join(", ", records) + "]}}";
    }

    static String record(String date, String close, String prevClose, String rate) {
        return "{\"dt\": \"" + date + "\", \"cls_prc_p2\": \"" + close + "\", \"bdy_cls_prc_p2\": \"" + prevClose
                + "\", \"up_dwn_r_p2\": \"" + rate + "\"}";
    }
}

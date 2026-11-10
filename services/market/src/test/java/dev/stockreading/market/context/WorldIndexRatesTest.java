package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import dev.stockreading.market.event.ContextPayload;
import tools.jackson.databind.json.JsonMapper;

/** 지수 코드·이름은 가짜 값이다. 원문의 등락률 필드는 실제처럼 0.00으로 두어 계산값이 이를 쓰지 않는지 본다. */
class WorldIndexRatesTest {

    private static final OffsetDateTime AS_OF = OffsetDateTime.parse("2026-11-02T09:00:00+09:00");
    private static final OffsetDateTime SINCE = AS_OF.minusDays(4);

    private final WorldIndexRates rates = new WorldIndexRates(JsonMapper.builder().build());

    @Test
    void computesTheRateFromThePreviousBusinessDayCloseInsteadOfTheZeroInTheResponse() {
        String current = response("TESTIX:20261030:  102.00", "FAKEIX:20261030:   49.00");
        List<BackgroundRepository.StoredResponse> rows = List.of(
                row(current, "2026-11-02T08:30:00+09:00"),
                row(response("TESTIX:20261029:  100.00", "FAKEIX:20261029:   50.00"), "2026-10-30T08:30:00+09:00"));

        List<ContextPayload.IndexRate> result = rates.compute(current, rows, SINCE, AS_OF);

        assertThat(result).containsExactly(
                new ContextPayload.IndexRate("TESTIX", "TESTIX INDEX", "20261030", "102.00", "20261029", "100.00",
                        "2.00"),
                new ContextPayload.IndexRate("FAKEIX", "FAKEIX INDEX", "20261030", "49.00", "20261029", "50.00",
                        "-2.00"));
    }

    @Test
    void rowWithTheSameBusinessDayIsNotThePreviousRow() {
        // 같은 영업일(10/30)을 담은 더 최근 행이 있어도 건너뛰고, 영업일이 더 이른 행 중 가장 최근에 받은 것을 쓴다.
        String current = response("TESTIX:20261030:  102.00");
        List<BackgroundRepository.StoredResponse> rows = List.of(
                row(current, "2026-11-02T08:30:00+09:00"),
                row(response("TESTIX:20261030:  101.00"), "2026-10-31T08:30:00+09:00"),
                row(response("TESTIX:20261029:  100.00"), "2026-10-30T16:30:00+09:00"),
                row(response("TESTIX:20261028:   80.00"), "2026-10-30T08:30:00+09:00"));

        ContextPayload.IndexRate rate = rates.compute(current, rows, SINCE, AS_OF).get(0);

        assertThat(rate.prevBusinessDay()).isEqualTo("20261029");
        assertThat(rate.prevClose()).isEqualTo("100.00");
        assertThat(rate.rate()).isEqualTo("2.00");
    }

    @Test
    void rowFetchedAfterTheReferenceTimeIsNotACandidate() {
        String current = response("TESTIX:20261030:  102.00");
        List<BackgroundRepository.StoredResponse> rows = List.of(
                row(response("TESTIX:20261029:   90.00"), "2026-11-02T09:00:01+09:00"),
                row(current, "2026-11-02T08:30:00+09:00"),
                row(response("TESTIX:20261029:  100.00"), "2026-11-02T09:00:00+09:00"));

        // 기준 시각과 같은 시각에 받은 행은 쓰고, 1초 뒤에 받은 행은 쓰지 않는다.
        assertThat(rates.compute(current, rows, SINCE, AS_OF).get(0).prevClose()).isEqualTo("100.00");
    }

    @Test
    void indexWithoutAPreviousRowInsideTheWindowHasNoRateWhileOthersAreComputed() {
        String current = response("TESTIX:20261030:  102.00", "FAKEIX:20261030:   49.00");
        List<BackgroundRepository.StoredResponse> rows = List.of(
                row(current, "2026-11-02T08:30:00+09:00"),
                row(response("FAKEIX:20261029:   50.00"), "2026-10-30T08:30:00+09:00"),
                // 기준 기간(4일) 하한보다 1초 일찍 받은 행이라 쓰지 않는다.
                row(response("TESTIX:20261027:  100.00"), "2026-10-29T08:59:59+09:00"));

        List<ContextPayload.IndexRate> result = rates.compute(current, rows, SINCE, AS_OF);

        assertThat(result.get(0)).isEqualTo(
                new ContextPayload.IndexRate("TESTIX", "TESTIX INDEX", "20261030", "102.00", null, null, null));
        assertThat(result.get(1).rate()).isEqualTo("-2.00");
    }

    @Test
    void previousRowFetchedExactlyAtTheLowerBoundIsUsed() {
        String current = response("TESTIX:20261030:  102.00");
        List<BackgroundRepository.StoredResponse> rows = List.of(
                row(current, "2026-11-02T08:30:00+09:00"),
                row(response("TESTIX:20261027:  100.00"), "2026-10-29T09:00:00+09:00"));

        assertThat(rates.compute(current, rows, SINCE, AS_OF).get(0).rate()).isEqualTo("2.00");
    }

    @Test
    void lookbackOfSevenDaysIncludesARowFetchedExactlyThatEarlyAndExcludesOneSecondEarlier() {
        // 마감 후 판처럼 요청 범위가 짧아도 직전 영업일 행은 기준 시각 7일 전까지 찾는다(하한 포함).
        OffsetDateTime lowerBound = AS_OF.minusDays(7);
        String current = response("TESTIX:20261030:  102.00");

        List<BackgroundRepository.StoredResponse> atBound = List.of(
                row(current, "2026-11-02T08:30:00+09:00"),
                row(response("TESTIX:20261023:  100.00"), "2026-10-26T09:00:00+09:00"));
        List<BackgroundRepository.StoredResponse> oneSecondBefore = List.of(
                row(current, "2026-11-02T08:30:00+09:00"),
                row(response("TESTIX:20261023:  100.00"), "2026-10-26T08:59:59+09:00"));

        assertThat(rates.compute(current, atBound, lowerBound, AS_OF).get(0).rate()).isEqualTo("2.00");
        assertThat(rates.compute(current, oneSecondBefore, lowerBound, AS_OF).get(0).rate()).isNull();
    }

    @Test
    void roundsToTwoDecimalsAndGivesNoRateWhenThePreviousCloseIsZero() {
        String current = response("TESTIX:20261030:  100.00", "FAKEIX:20261030:    1.00");
        List<BackgroundRepository.StoredResponse> rows = List.of(
                row(response("TESTIX:20261029:   98.00", "FAKEIX:20261029:    0.00"), "2026-10-30T08:30:00+09:00"));

        List<ContextPayload.IndexRate> result = rates.compute(current, rows, SINCE, AS_OF);

        assertThat(result.get(0).rate()).isEqualTo("2.04");
        assertThat(result.get(1).prevClose()).isEqualTo("0.00");
        assertThat(result.get(1).rate()).isNull();
    }

    @Test
    void recordsThatCannotBeReadAreSkippedAndRowsThatAreNotJsonAreIgnored() {
        String current = """
                {"dataBody":{"out2":[{"is_cd":"TESTIX","bsnss_dy":"20261030","cls_prc_p2":" 102.00"},
                 {"is_cd":"BADDAY","bsnss_dy":"2026-10-30","cls_prc_p2":"1"},
                 {"is_cd":"BADCLS","bsnss_dy":"20261030","cls_prc_p2":"n/a"},
                 {"bsnss_dy":"20261030","cls_prc_p2":"1"}]}}""";
        List<BackgroundRepository.StoredResponse> rows = List.of(
                row("not json", "2026-10-31T08:30:00+09:00"),
                row(response("TESTIX:20261029:  100.00"), "2026-10-30T08:30:00+09:00"));

        List<ContextPayload.IndexRate> result = rates.compute(current, rows, SINCE, AS_OF);

        // 이름이 없는 지수는 빈 이름으로 싣는다.
        assertThat(result).containsExactly(
                new ContextPayload.IndexRate("TESTIX", "", "20261030", "102.00", "20261029", "100.00", "2.00"));
    }

    private static BackgroundRepository.StoredResponse row(String response, String fetchedAt) {
        return new BackgroundRepository.StoredResponse(response, OffsetDateTime.parse(fetchedAt));
    }

    /** "코드:영업일:종가" 목록으로 원문을 만든다. 전일대비·등락률은 장이 끝난 지수처럼 0.00이다. */
    private static String response(String... indexes) {
        String out2 = Arrays.stream(indexes)
                .map(i -> i.split(":", 3))
                .map(i -> "{\"is_cd\":\"" + i[0] + "  \",\"is_cd_nm\":\"" + i[0] + " INDEX\",\"bsnss_dy\":\"" + i[1]
                        + "\",\"ttm\":\"9999\",\"cls_prc_p2\":\"" + i[2] + "\",\"bdy_cmpr_p2\":\"    0.00\","
                        + "\"bdy_cmpr_r_p2\":\"    0.00\"}")
                .collect(Collectors.joining(","));
        return "{\"dataHeader\":{\"resultCode\":\"200\"},\"dataBody\":{\"out2\":[" + out2 + "]}}";
    }
}

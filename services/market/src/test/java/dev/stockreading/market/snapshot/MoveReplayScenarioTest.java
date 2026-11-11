package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import dev.stockreading.market.MarketProperties;

/**
 * 2026-11-11 장중에 나온 가짜 급변동·중복 발행의 모양을 가짜 종목과 가짜 값으로 만들어, 기본 설정(application.yml)으로
 * 실제 MoveDetector·MoveTracker 에 받은 순서대로 넣고 market.move 건수를 센다. 환경변수 없이 항상 돈다.
 * 한 줄은 1초 현재가 한 건이고, 시각은 가짜 날짜의 09:00:00부터 초 단위로 센다.
 */
class MoveReplayScenarioTest {

    private static final OffsetDateTime DAY0 = OffsetDateTime.parse("2026-01-05T09:00:00+09:00");

    @Configuration
    @EnableConfigurationProperties(MarketProperties.class)
    static class Wiring {
    }

    /** 한 구간의 1초 행들. 전일 종가 후보 칸은 세 칸 모두 prevClose, 시가와 누적 거래량은 원문 칸에 그대로 넣는다. */
    private record Seg(int from, int to, long price, long prevClose, long open, long volume) {
    }

    private static Seg seg(int from, int to, long price, long prevClose, long open, long volume) {
        return new Seg(from, to, price, prevClose, open, volume);
    }

    private static List<String> rows(String symbol, Seg... segs) {
        List<String> lines = new ArrayList<>();
        for (Seg seg : segs) {
            for (int s = seg.from(); s <= seg.to(); s++) {
                lines.add(line(symbol, s, seg.price(), seg.prevClose(), seg.prevClose(), seg.prevClose(), seg.open(), seg.volume()));
            }
        }
        return lines;
    }

    // 재생은 eventId 를 쓰지 않아 초를 번호로 쓴다.
    private static String line(String symbol, int second, long price, long pc1, long pc2, long pc3, long open, long volume) {
        String raw = "{\"dataBody\":{\"now_prc\":\"" + price + "\",\"bdy_cls_prc\":\"" + pc1 + "\",\"sprc\":\"" + pc2
                + "\",\"krx_bdy_clpr\":\"" + pc3 + "\",\"opn_prc\":\"" + open + "\",\"acml_vlm\":\"" + volume + "\"}}";
        return String.format("00000000-0000-4000-8000-%012d\t%s\t%s\t%d\t%s", second, symbol, DAY0.plusSeconds(second), price, raw);
    }

    private static int moves(List<String> lines, Path dir) throws IOException {
        Path input = dir.resolve("snapshots.tsv");
        Files.writeString(input, lines.stream().collect(Collectors.joining("\n", "", "\n")));
        int[] total = new int[1];
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .run(context -> {
                    MoveReplayTest.Result result = MoveReplayTest.replay(context.getBean(MarketProperties.class), input, Set.of());
                    total[0] = result.published().values().stream().mapToInt(Integer::intValue).sum();
                });
        return total[0];
    }

    // 거의 평탄한 값 중 한 행만 약 +7.5% 튀고(전일 종가 칸 둘도 함께 깨짐) 곧 제자리로 돌아온다.
    // 예전에는 튄 행이 들어올 때 약 +7.5%, 5분 뒤 튄 행이 비교 기준이 될 때 약 -7%로 두 건이 나갔다.
    @Test
    void aSingleSpikedRowIsNotPublishedNeitherWhenItEntersNorFiveMinutesLater(@TempDir Path dir) throws IOException {
        List<String> lines = rows("TEST01", seg(0, 112, 10_000, 9_995, 10_040, 500));
        lines.add(line("TEST01", 113, 10_755, 10_740, 9_995, 10_740, 10_040, 500));
        lines.addAll(rows("TEST01", seg(114, 480, 10_000, 9_995, 10_040, 500)));

        assertThat(moves(lines, dir)).isZero();
    }

    // 장 시작 직후 첫 체결 전(현재가가 전일 종가 그대로, 시가·누적 거래량 0) 23초 뒤 첫 체결이 오고 오르내린다.
    // 예전에는 첫 체결 전 값이 5분 동안 기준이 되어 1% 안팎을 오갈 때마다 세 건이 나갔다.
    @Test
    void rowsBeforeTheFirstTradeAreNotTheBaselineSoTheOpeningProducesAtMostOneMove(@TempDir Path dir) throws IOException {
        long pc = 10_000;
        List<String> lines = rows("TEST01",
                seg(0, 22, 10_000, pc, 0, 0), seg(23, 131, 10_153, pc, 10_153, 1_000),
                seg(132, 141, 9_936, pc, 10_153, 3_000), seg(142, 229, 9_927, pc, 10_153, 3_300),
                seg(230, 287, 9_858, pc, 10_153, 3_400), seg(288, 303, 9_925, pc, 10_153, 3_500),
                seg(304, 420, 9_914, pc, 10_153, 4_300));

        assertThat(moves(lines, dir)).isLessThanOrEqualTo(1);
    }

    @Test
    void aRealMoveThatLastsIsPublishedOnce(@TempDir Path dir) throws IOException {
        List<String> lines = rows("TEST01", seg(0, 399, 10_000, 10_000, 10_000, 1_000), seg(400, 459, 10_200, 10_000, 10_000, 2_000));

        assertThat(moves(lines, dir)).isEqualTo(1);
    }

    @Test
    void aMoveThatLastsOnlyTwoRowsIsNotPublished(@TempDir Path dir) throws IOException {
        List<String> lines = rows("TEST01", seg(0, 399, 10_000, 10_000, 10_000, 1_000),
                seg(400, 401, 10_200, 10_000, 10_000, 2_000), seg(402, 460, 10_000, 10_000, 10_000, 2_100));

        assertThat(moves(lines, dir)).isZero();
    }

    @Test
    void aMoveFollowedByAnotherWithinThirtyMinutesIsHeldBackUnlessItGoesTwoPercentFurther(@TempDir Path dir) throws IOException {
        // 발행(10200) 뒤 약 10분에 같은 방향으로 +1.47%
        List<String> held = rows("TEST01", seg(0, 399, 10_000, 10_200, 10_000, 1_000),
                seg(400, 999, 10_200, 10_200, 10_000, 2_000), seg(1_000, 1_199, 10_350, 10_200, 10_000, 3_000));
        assertThat(moves(held, dir)).isEqualTo(1);

        // 같은 자리에서 +2.45%이면 간격 안이라도 발행한다.
        List<String> further = rows("TEST01", seg(0, 399, 10_000, 10_200, 10_000, 1_000),
                seg(400, 999, 10_200, 10_200, 10_000, 2_000), seg(1_000, 1_199, 10_450, 10_200, 10_000, 3_000));
        assertThat(moves(further, dir)).isEqualTo(2);

        // 31분이 지나면 +1.47%도 다시 발행한다.
        List<String> later = rows("TEST01", seg(0, 399, 10_000, 10_200, 10_000, 1_000),
                seg(400, 2_299, 10_200, 10_200, 10_000, 2_000), seg(2_300, 2_399, 10_350, 10_200, 10_000, 3_000));
        assertThat(moves(later, dir)).isEqualTo(2);
    }

    @Test
    void theOtherTypeIsHeldBackWithinTheInterval(@TempDir Path dir) throws IOException {
        // 5분 비교 변동을 낸 뒤 10분에 전일 종가 대비 +3.2% 조건이 생겨도 마지막 발행 가격 대비 +1.18%라 내지 않는다.
        List<String> lines = rows("TEST01", seg(0, 399, 10_000, 10_000, 10_000, 1_000),
                seg(400, 999, 10_200, 10_000, 10_000, 2_000), seg(1_000, 1_199, 10_320, 10_000, 10_000, 3_000));

        assertThat(moves(lines, dir)).isEqualTo(1);
    }

    @Test
    void aMoveHeldBackByTheIntervalIsNotPublishedWhenTheIntervalEnds(@TempDir Path dir) throws IOException {
        // +4%로 뛰어 5분 비교와 전일 종가 비교 조건이 함께 생긴다. 5분 비교만 발행되고 전일 종가 조건은 40분 넘게 이어진다.
        List<String> lines = rows("TEST01", seg(0, 399, 10_000, 10_000, 10_000, 1_000), seg(400, 2_900, 10_400, 10_000, 10_000, 2_000));
        assertThat(moves(lines, dir)).isEqualTo(1);

        // 같은 자리에서 간격 안에 +2.4%를 더 가면 발행한다.
        List<String> further = rows("TEST01", seg(0, 399, 10_000, 10_000, 10_000, 1_000),
                seg(400, 999, 10_400, 10_000, 10_000, 2_000), seg(1_000, 2_900, 10_650, 10_000, 10_000, 3_000));
        assertThat(moves(further, dir)).isEqualTo(2);
    }

    @Test
    void aMoveAgainstThePreviousCloseAloneIsPublishedWhenItLasts(@TempDir Path dir) throws IOException {
        List<String> lines = rows("TEST01", seg(0, 399, 10_250, 10_000, 10_250, 1_000), seg(400, 459, 10_310, 10_000, 10_250, 2_000));

        assertThat(moves(lines, dir)).isEqualTo(1);
    }
}

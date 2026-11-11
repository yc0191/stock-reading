package dev.stockreading.market.snapshot;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.stockreading.market.MarketProperties;

// MoveReplayTest 의 재생 집계(날짜별 유형 2 발행·거짓 건수)를 가짜 데이터로 확인한다. 환경변수 없이 항상 돈다.
// 집계만 보려고 연속 확인(1건)과 재발행 간격(끔)은 꺼 둔다. 두 규칙은 MoveTrackerTest·MoveDetectorTest 가 확인한다.
class MoveReplayCountTest {

    // 가짜 종목·가짜 값. 4일에 한 행이 깨져 두 칸이 같은 엉뚱한 값을 내면 유형 2가 거짓으로 나오고, 5일은 진짜 변동이다.
    @Test
    void countsFalsePrevCloseMovesByDate(@TempDir Path dir) throws IOException {
        MarketProperties properties = new MarketProperties(
                5, new BigDecimal("1.00"), List.of("f.a", "f.b", "f.c"), new BigDecimal("3.00"), List.of(),
                1, List.of(), 0, new BigDecimal("2.00"));
        Path input = dir.resolve("snapshots.tsv");
        Files.writeString(input, String.join("\n",
                row("e1", "2026-11-04T10:00:00+09:00", 10_000, 10_000, 10_000, 10_000),
                row("e2", "2026-11-04T10:01:00+09:00", 10_100, 7_000, 7_000, 10_000),
                row("e3", "2026-11-05T10:00:00+09:00", 10_000, 10_000, 10_000, 10_000),
                row("e4", "2026-11-05T10:01:00+09:00", 10_400, 10_000, 10_000, 10_000)) + "\n");

        MoveReplayTest.Result result = MoveReplayTest.replay(properties, input, Set.of());

        assertThat(result.prevCloseByDate().keySet()).containsExactly("2026-11-04", "2026-11-05");
        assertThat(result.prevCloseByDate().get("2026-11-04")).containsExactly(1, 1);
        assertThat(result.prevCloseByDate().get("2026-11-05")).containsExactly(1, 0);
    }

    private static String row(String id, String asOf, long price, long a, long b, long c) {
        return id + "\tFAKE01\t" + asOf + "\t" + price + "\t{\"f\":{\"a\":\"" + a + "\",\"b\":\"" + b + "\",\"c\":\"" + c + "\"}}";
    }
}

package dev.stockreading.market.event;

import java.util.List;

/** market.history.responded의 payload. jobId와 seq는 요청 값을 그대로 되돌려준다. */
public record HistoryPayload(String jobId, int seq, String symbol, List<Point> points, boolean truncated) {

    public record Point(String asOf, long price) {
    }
}

package dev.stockreading.market.event;

import java.util.List;

/** 내부 조회 API 의 응답 본문(옛 market.history 응답 이벤트의 payload 와 같은 모양). jobId와 seq는 요청 값을 그대로 되돌려준다. */
public record HistoryPayload(String jobId, int seq, String symbol, List<Point> points, boolean truncated) {

    public record Point(String asOf, long price) {
    }
}

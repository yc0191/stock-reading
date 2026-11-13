package dev.stockreading.market.history;

import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.stereotype.Component;

import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.event.HistoryPayload;

@Component
class HistoryQuery {

    /**
     * 요청이 몇 건을 달라고 하든 이 건수까지만 읽는다. 응답 본문이 끝없이 커지지 않게 한다(행 하나는 약 50바이트).
     * Kafka 응답 때 메시지 한도(기본 1MB)에 맞춰 정한 값을 그대로 두어, 같은 요청에 예전과 같은 내용을 준다.
     */
    static final int MAX_POINTS_CAP = 5000;

    private final PriceHistoryRepository repository;

    HistoryQuery(PriceHistoryRepository repository) {
        this.repository = repository;
    }

    /** 내부 조회 API 가 부른다. 같은 요청에는 같은 내용을 돌려준다. */
    HistoryPayload answer(HistoryRequest request) {
        OffsetDateTime since = OffsetDateTime.parse(request.since());
        OffsetDateTime until = OffsetDateTime.parse(request.until());
        Integer interval = request.intervalSeconds();
        if (request.maxPoints() < 1 || since.isAfter(until) || (interval != null && interval < 1)) {
            // 잘못된 요청은 답하지 않는다. 내부 조회 API 는 먼저 400 으로 거른다.
            throw new IllegalArgumentException("invalid history request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }

        int limit = Math.min(request.maxPoints(), MAX_POINTS_CAP);
        // 한 건 더 읽어 잘렸는지 판단한다. 잘리면 뒤쪽을 버려, 기준이 되는 구간 앞쪽 가격을 남긴다.
        // 솎는 간격이 없는 이전 형식 요청은 솎지 않고 지금처럼 모든 점을 준다.
        List<PriceHistoryRepository.StoredPoint> rows = interval == null
                ? repository.findBetweenInclusive(request.symbol(), since, until, limit + 1)
                : repository.findThinnedBetweenInclusive(request.symbol(), since, until, interval, limit + 1);
        boolean truncated = rows.size() > limit;
        List<HistoryPayload.Point> points = rows.stream()
                .limit(limit)
                .map(row -> new HistoryPayload.Point(EventFactory.formatKst(row.asOf()), row.price()))
                .toList();
        return new HistoryPayload(request.jobId(), request.seq(), request.symbol(), points, truncated);
    }
}

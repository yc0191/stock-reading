package dev.stockreading.market.history;

import java.time.OffsetDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.event.HistoryPayload;
import tools.jackson.databind.ObjectMapper;

@Component
class HistoryRequestListener {

    /**
     * 요청이 몇 건을 달라고 하든 이 건수까지만 읽는다. 응답 한 건이 Kafka 메시지 한도(기본 1MB)를 넘으면
     * 발행이 거부되어 브리핑이 응답을 영영 받지 못하기 때문이다(행 하나는 약 50바이트).
     */
    static final int MAX_POINTS_CAP = 5000;

    private static final Logger log = LoggerFactory.getLogger(HistoryRequestListener.class);

    private final PriceHistoryRepository repository;
    private final HistoryPublisher publisher;
    private final ObjectMapper mapper;

    HistoryRequestListener(PriceHistoryRepository repository, HistoryPublisher publisher, ObjectMapper mapper) {
        this.repository = repository;
        this.publisher = publisher;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.MARKET_HISTORY_REQUESTED, groupId = "market")
    void onRequest(String message) {
        HistoryRequestEnvelope event = mapper.readValue(message, HistoryRequestEnvelope.class);
        HistoryRequestEnvelope.RequestPayload request = event.payload();
        OffsetDateTime since = OffsetDateTime.parse(request.since());
        OffsetDateTime until = OffsetDateTime.parse(request.until());
        if (request.maxPoints() < 1 || since.isAfter(until)) {
            // 잘못된 요청은 응답하지 않는다. 브리핑이 제한 시간 뒤 다시 보내거나 실패로 기록한다.
            throw new IllegalArgumentException("invalid history request jobId=" + request.jobId()
                    + " seq=" + request.seq());
        }

        int limit = Math.min(request.maxPoints(), MAX_POINTS_CAP);
        // 한 건 더 읽어 잘렸는지 판단한다. 잘리면 뒤쪽을 버려, 기준이 되는 구간 앞쪽 가격을 남긴다.
        List<PriceHistoryRepository.StoredPoint> rows =
                repository.findBetweenInclusive(request.symbol(), since, until, limit + 1);
        boolean truncated = rows.size() > limit;
        List<HistoryPayload.Point> points = rows.stream()
                .limit(limit)
                .map(row -> new HistoryPayload.Point(EventFactory.format(row.asOf()), row.price()))
                .toList();

        publisher.publish(request.jobId(), request.seq(), request.symbol(), points, truncated, event.correlationId());
    }
}

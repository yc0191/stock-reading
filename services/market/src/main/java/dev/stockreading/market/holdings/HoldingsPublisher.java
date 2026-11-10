package dev.stockreading.market.holdings;

import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import dev.stockreading.market.Topics;
import dev.stockreading.market.event.Envelope;
import dev.stockreading.market.event.EventFactory;
import dev.stockreading.market.event.HoldingsPayload;
import tools.jackson.databind.ObjectMapper;

@Component
class HoldingsPublisher {

    private static final Logger log = LoggerFactory.getLogger(HoldingsPublisher.class);

    /** 압축 토픽이라 키가 같아야 마지막 목록 하나만 남는다. */
    static final String KEY = "holdings";

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final Clock clock;

    HoldingsPublisher(KafkaTemplate<String, String> kafka, ObjectMapper mapper, Clock clock) {
        this.kafka = kafka;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * 전체 목록을 발행하고 브로커가 받을 때까지 기다린다. 실패하면 예외를 던져 호출한 쪽이 다음 확인 때 다시 보내게 한다.
     * 종목코드·티커는 보유 종목이 드러나므로 건수만 남긴다.
     */
    void publish(HoldingsSnapshot snapshot) throws Exception {
        OffsetDateTime now = OffsetDateTime.now(clock);
        // 요청이 없는 이벤트라 이어받을 correlationId가 없어 확인 회차마다 새로 만든다.
        Envelope<HoldingsPayload> event = EventFactory.create(clock, Topics.MARKET_HOLDINGS, now,
                "holdings-change-" + UUID.randomUUID(), snapshot.toPayload());
        kafka.send(Topics.MARKET_HOLDINGS, KEY, mapper.writeValueAsString(event))
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info("holdings published instruments={} memberships={} corr={}", snapshot.instruments().size(),
                snapshot.memberships().size(), event.correlationId());
    }
}

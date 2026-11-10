package dev.stockreading.collector.holdings;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.HoldingsEnvelope;
import tools.jackson.databind.ObjectMapper;

/**
 * 시세처리기가 알리는 보유 종목 목록(market.holdings)을 받아 최근 목록으로 바꾼다.
 * 이 토픽은 마지막 목록만 남는 압축 토픽이라, 시작할 때 처음부터 읽어야 마지막 목록을 받는다. 설정에 있는 값으로 대신하지 않는다.
 */
@Component
class HoldingsListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(HoldingsListener.class);

    private final ReceivedHoldings holdings;
    private final ObjectMapper mapper;

    HoldingsListener(ReceivedHoldings holdings, ObjectMapper mapper) {
        this.holdings = holdings;
        this.mapper = mapper;
    }

    // 읽은 위치가 이미 커밋된 그룹은 auto.offset.reset 이 쓰이지 않아 재시작하면 마지막 목록을 다시 받지 못한다.
    // 목록은 메모리에만 두므로 파티션을 배정받을 때마다 처음부터 읽는다(키 하나인 압축 토픽이라 읽을 양은 거의 없다).
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        callback.seekToBeginning(assignments.keySet());
    }

    // schedule.tick 리스너들과 그룹을 나눠 둔다. 같은 그룹이면 토픽 배정이 섞여 어느 한쪽이 메시지를 놓칠 수 있다.
    @KafkaListener(topics = Topics.MARKET_HOLDINGS, groupId = "collector-holdings",
            properties = "auto.offset.reset=earliest")
    void onHoldings(String message) {
        HoldingsEnvelope event = mapper.readValue(message, HoldingsEnvelope.class);
        if (event.payload() == null || event.payload().instruments() == null
                || event.payload().memberships() == null) {
            // 깨진 목록으로 지금 목록을 지우지 않고 이전 목록을 그대로 둔다.
            log.warn("invalid holdings event ignored eventId={} corr={}", event.eventId(), event.correlationId());
            return;
        }
        holdings.update(event.payload());
    }
}

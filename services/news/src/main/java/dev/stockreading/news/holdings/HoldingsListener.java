package dev.stockreading.news.holdings;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import dev.stockreading.news.Topics;
import dev.stockreading.news.event.HoldingsEnvelope;
import dev.stockreading.news.searchquery.GeneratedSearchQueries;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * 시세처리기가 알리는 보유 종목 목록(market.holdings)을 받아 구성 관계를 바꾸고, 새 구성 종목의 검색어를 만든다.
 * 이 토픽은 마지막 목록만 남는 압축 토픽이라, 시작할 때 처음부터 읽어야 마지막 목록을 받는다.
 */
@Component
class HoldingsListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(HoldingsListener.class);

    private final ReceivedHoldings holdings;
    private final GeneratedSearchQueries generatedQueries;
    private final ObjectMapper mapper;

    HoldingsListener(ReceivedHoldings holdings, GeneratedSearchQueries generatedQueries, ObjectMapper mapper) {
        this.holdings = holdings;
        this.generatedQueries = generatedQueries;
        this.mapper = mapper;
    }

    // 읽은 위치가 이미 커밋된 그룹은 auto.offset.reset 이 쓰이지 않아 재시작하면 마지막 목록을 다시 받지 못한다.
    // 목록은 메모리에만 두므로 파티션을 배정받을 때마다 처음부터 읽는다(키 하나인 압축 토픽이라 읽을 양은 거의 없다).
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        callback.seekToBeginning(assignments.keySet());
    }

    // 다른 리스너와 그룹을 나눠 둔다. 같은 그룹이면 토픽 배정이 섞여 어느 한쪽이 메시지를 놓칠 수 있다.
    // 재시작하면 압축되지 않은 옛 목록이 여러 개 오므로 묶음으로 받아, 검색어는 가장 최신 목록으로만 만든다.
    // 옛 목록마다 만들면 이미 빠진 종목이나 작업자가 지운 자동 검색어가 되살아난다.
    @KafkaListener(topics = Topics.MARKET_HOLDINGS, groupId = "news-holdings", batch = "true",
            properties = "auto.offset.reset=earliest")
    void onHoldings(List<String> messages) {
        HoldingsEnvelope.HoldingsPayload latest = null;
        for (String message : messages) {
            HoldingsEnvelope event;
            try {
                event = mapper.readValue(message, HoldingsEnvelope.class);
            } catch (JacksonException e) {
                // 묶음째 실패하면 같은 묶음의 정상 목록까지 버려지므로 읽지 못한 메시지 하나만 건너뛴다.
                log.warn("unreadable holdings event ignored in a batch of {}", messages.size());
                continue;
            }
            if (event.payload() == null || event.payload().instruments() == null
                    || event.payload().memberships() == null) {
                // 깨진 목록으로 지금 목록을 지우지 않고 이전 목록을 그대로 둔다.
                log.warn("invalid holdings event ignored eventId={} corr={}", event.eventId(), event.correlationId());
                continue;
            }
            holdings.update(event.payload());
            latest = event.payload();
        }
        if (latest != null) {
            generatedQueries.generate(latest);
        }
    }
}

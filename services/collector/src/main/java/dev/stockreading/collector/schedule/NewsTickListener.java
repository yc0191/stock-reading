package dev.stockreading.collector.schedule;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.TickEnvelope;
import tools.jackson.databind.ObjectMapper;

@Component
class NewsTickListener {

    private static final Logger log = LoggerFactory.getLogger(NewsTickListener.class);
    private static final int MAX_REMEMBERED_TICKS = 1_000;

    private final NewsPublisher publisher;
    private final ObjectMapper mapper;

    // 메모리 기반 중복 방지(TickListener 와 같은 방식). 재시작하면 초기화된다.
    private final Set<String> processedTicks = ConcurrentHashMap.newKeySet();

    NewsTickListener(NewsPublisher publisher, ObjectMapper mapper) {
        this.publisher = publisher;
        this.mapper = mapper;
    }

    // schedule.tick 은 파티션이 1개라서, TickListener 와 같은 그룹이면 둘 중 하나만 메시지를 받아
    // 오류 없이 tick 이 누락된다. 그래서 그룹을 분리한다.
    @KafkaListener(topics = Topics.SCHEDULE_TICK, groupId = "collector-news")
    void onTick(String message) {
        TickEnvelope tick = mapper.readValue(message, TickEnvelope.class);
        String jobType = tick.payload().jobType();
        if (!TickPublisher.JOB_COLLECT_NEWS.equals(jobType)) {
            log.debug("ignoring tick with jobType={}", jobType);
            return;
        }
        String dedupeKey = jobType + "|" + tick.payload().scheduledFor();
        if (!processedTicks.add(dedupeKey)) {
            log.info("duplicate tick ignored key={}", dedupeKey);
            return;
        }
        if (processedTicks.size() > MAX_REMEMBERED_TICKS) {
            processedTicks.clear();
            processedTicks.add(dedupeKey);
        }

        publisher.publishCollected(tick.correlationId(), tick.payload().scheduledFor());
    }
}

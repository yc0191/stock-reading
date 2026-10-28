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
class FilingTickListener {

    private static final Logger log = LoggerFactory.getLogger(FilingTickListener.class);
    private static final int MAX_REMEMBERED_TICKS = 1_000;

    private final FilingPublisher publisher;
    private final ObjectMapper mapper;

    // 메모리 기반 중복 방지(TickListener 와 같은 방식). 재시작하면 초기화된다.
    private final Set<String> processedTicks = ConcurrentHashMap.newKeySet();

    FilingTickListener(FilingPublisher publisher, ObjectMapper mapper) {
        this.publisher = publisher;
        this.mapper = mapper;
    }

    // schedule.tick 은 파티션이 1개라서 다른 리스너와 그룹이 같으면 tick 이 오류 없이 누락된다.
    @KafkaListener(topics = Topics.SCHEDULE_TICK, groupId = "collector-filings")
    void onTick(String message) {
        TickEnvelope tick = mapper.readValue(message, TickEnvelope.class);
        String jobType = tick.payload().jobType();
        if (!TickPublisher.JOB_COLLECT_FILINGS.equals(jobType)) {
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

        publisher.publishCollected(tick.correlationId());
    }
}

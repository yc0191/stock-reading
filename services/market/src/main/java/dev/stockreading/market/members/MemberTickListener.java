package dev.stockreading.market.members;

import java.time.Clock;
import java.time.LocalDate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import dev.stockreading.market.Topics;
import tools.jackson.databind.ObjectMapper;

/**
 * 구성 종목 조회 tick(collect-members)을 받아 오늘 받을 차례인 ETF의 구성을 갱신한다.
 * 수집기가 평일 하루 한 번 tick을 내고, 그날 실제로 받을지는 MemberFetchService가 정한다.
 */
@Component
class MemberTickListener {

    static final String JOB_COLLECT_MEMBERS = "collect-members";

    private static final Logger log = LoggerFactory.getLogger(MemberTickListener.class);
    private static final int MAX_REMEMBERED_TICKS = 1_000;

    private final MemberFetchService service;
    private final ObjectMapper mapper;
    private final Clock clock;

    // 메모리 기반 중복 방지(수집기 리스너와 같은 방식). 재시작하면 초기화되고, 다시 돌려도 결과는 같다.
    private final Set<String> processedTicks = ConcurrentHashMap.newKeySet();

    MemberTickListener(MemberFetchService service, ObjectMapper mapper, Clock clock) {
        this.service = service;
        this.mapper = mapper;
        this.clock = clock;
    }

    // schedule.tick은 파티션이 1개라서 같은 그룹의 다른 리스너와 나눠 받으면 tick이 오류 없이 누락된다. 이 리스너만의 그룹을 쓴다.
    @KafkaListener(topics = Topics.SCHEDULE_TICK, groupId = "market-members")
    void onTick(String message) {
        TickEnvelope tick = mapper.readValue(message, TickEnvelope.class);
        String jobType = tick.payload().jobType();
        if (!JOB_COLLECT_MEMBERS.equals(jobType)) {
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
        service.run(LocalDate.now(clock));
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record TickEnvelope(String eventId, String eventType, int schemaVersion, String occurredAt, String producedAt,
            String correlationId, TickPayload payload) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record TickPayload(String jobType, String scheduledFor) {
    }
}

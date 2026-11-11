package dev.stockreading.collector.schedule;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.CommandEnvelope;
import dev.stockreading.collector.schedule.CollectorJobs.Job;
import dev.stockreading.collector.schedule.CollectorJobs.Outcome;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * 사람이 보내는 수동 실행 명령(collector.command)을 받아 그 수집 작업을 지금 1회 실행한다(ADR-012).
 * 타이머와 같은 실행 입구(CollectorJobs)를 쓰며, 시세 시간대와 타이머 켜기 설정은 보지 않는다.
 * 같은 작업이 돌고 있으면 건너뛰고, 모르는 명령은 무시한다. 둘 다 로그만 남긴다.
 */
@Component
class CollectorCommandListener implements ConsumerSeekAware {

    private static final Logger log = LoggerFactory.getLogger(CollectorCommandListener.class);
    private static final int MAX_REMEMBERED_COMMANDS = 1_000;

    private final CollectorJobs jobs;
    private final ObjectMapper mapper;

    // 메모리 기반 중복 방지. 재시작하면 초기화되지만, 재시작 전 명령은 아래 seekToEnd 로 다시 읽지 않는다.
    private final Set<String> processedCommands = ConcurrentHashMap.newKeySet();

    CollectorCommandListener(CollectorJobs jobs, ObjectMapper mapper) {
        this.jobs = jobs;
        this.mapper = mapper;
    }

    // 읽은 위치가 커밋된 그룹은 재시작하면 꺼져 있던 동안 쌓인 명령부터 읽어 한꺼번에 실행한다.
    // 명령은 "지금" 실행하라는 뜻이므로 파티션을 배정받을 때마다 끝으로 옮겨, 켜진 뒤에 보낸 명령만 실행한다.
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        callback.seekToEnd(assignments.keySet());
    }

    // 다른 리스너와 그룹을 나눠 둔다. 같은 그룹이면 토픽 배정이 섞여 어느 한쪽이 메시지를 놓칠 수 있다.
    // 작업은 새 스레드에서 돌고 리스너는 바로 돌아온다. 오래 걸리는 작업(마감 후 등)이 poll 간격을 넘기지 않게 한다.
    @KafkaListener(topics = Topics.COLLECTOR_COMMAND, groupId = "collector-command")
    void onCommand(String message) {
        CommandEnvelope command;
        try {
            command = mapper.readValue(message, CommandEnvelope.class);
        } catch (JacksonException e) {
            // 다시 읽어도 같은 결과라 재시도하지 않고 버린다.
            log.warn("unreadable collector command ignored errorType={}", e.getClass().getSimpleName());
            return;
        }
        // 본문이 JSON null 이면 예외 없이 null 이 돌아온다.
        if (command == null) {
            log.warn("unreadable collector command ignored errorType=null");
            return;
        }
        String eventId = command.eventId();
        String name = command.payload() == null ? null : command.payload().command();
        Optional<Job> job = Job.of(name);
        if (eventId == null || job.isEmpty()) {
            log.warn("unknown collector command ignored eventId={} command={}", eventId, name);
            return;
        }
        if (!processedCommands.add(eventId)) {
            log.info("duplicate collector command ignored eventId={} command={}", eventId, name);
            return;
        }
        if (processedCommands.size() > MAX_REMEMBERED_COMMANDS) {
            processedCommands.clear();
            processedCommands.add(eventId);
        }

        Outcome outcome = jobs.startOnDemand(job.get());
        if (outcome == Outcome.ALREADY_RUNNING) {
            log.warn("collector command skipped, job already running eventId={} command={}", eventId, name);
            return;
        }
        log.info("collector command started eventId={} command={} corr={}", eventId, name, command.correlationId());
    }
}

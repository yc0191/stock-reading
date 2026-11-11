package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware.ConsumerSeekCallback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.schedule.CollectorJobs.Job;
import dev.stockreading.collector.schedule.CollectorJobs.Outcome;
import tools.jackson.databind.json.JsonMapper;

class CollectorCommandListenerTest {

    private static final String SAMPLE_EVENT_ID = "6f1d2c3b-4a59-4e7f-8d10-2b3c4d5e6f70";

    private CollectorJobs jobs;
    private CollectorCommandListener listener;
    private ListAppender<ILoggingEvent> logs;

    @BeforeEach
    void setUp() {
        jobs = mock(CollectorJobs.class);
        when(jobs.startOnDemand(any())).thenReturn(Outcome.STARTED);
        listener = new CollectorCommandListener(jobs, JsonMapper.builder().build());
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(CollectorCommandListener.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(CollectorCommandListener.class)).detachAppender(logs);
    }

    @Test
    void eachCommandStartsItsJobOnceThroughTheOnDemandEntrance() throws IOException {
        for (Job job : Job.values()) {
            listener.onCommand(command(UUID.randomUUID().toString(), job.jobType()));

            verify(jobs).startOnDemand(job);
        }
        // 타이머용 입구(건너뜀 이벤트를 남기는 쪽)는 쓰지 않는다.
        verify(jobs, never()).start(any());
        verify(jobs, never()).run(any());
    }

    @Test
    void theSameEventIdRunsOnlyOnce() throws IOException {
        String sample = ContractSchemas.sample("collector.command.sample.json");

        listener.onCommand(sample);
        listener.onCommand(sample);

        verify(jobs).startOnDemand(Job.COLLECT);
        assertThat(messages(Level.INFO)).anySatisfy(m -> assertThat(m)
                .contains("duplicate collector command ignored").contains(SAMPLE_EVENT_ID));
    }

    @Test
    void unknownBrokenOrEmptyCommandsAreIgnoredWithALogAndDoNotThrow() throws IOException {
        String sample = ContractSchemas.sample("collector.command.sample.json");
        List<String> bad = List.of(
                command(UUID.randomUUID().toString(), "collect-members"),
                command(UUID.randomUUID().toString(), "daily-briefing-morning"),
                sample.replace("\"command\": \"collect\"", "\"jobType\": \"collect\""),
                sample.replace("\"eventId\": \"" + SAMPLE_EVENT_ID + "\",", ""),
                "{not json");

        for (String message : bad) {
            // 예외가 나면 리스너 컨테이너가 같은 메시지를 다시 시도하므로 예외 없이 끝나야 한다.
            assertThatCode(() -> listener.onCommand(message)).doesNotThrowAnyException();
        }

        verifyNoInteractions(jobs);
        assertThat(messages(Level.WARN)).hasSize(bad.size())
                .allSatisfy(m -> assertThat(m).containsAnyOf("unknown collector command ignored",
                        "unreadable collector command ignored"));
        assertThat(messages(Level.WARN)).anySatisfy(m -> assertThat(m).contains("command=collect-members"));
    }

    @Test
    void aJsonNullBodyIsIgnoredWithALogAndDoesNotThrow() {
        assertThatCode(() -> listener.onCommand("null")).doesNotThrowAnyException();

        verifyNoInteractions(jobs);
        assertThat(messages(Level.WARN)).singleElement()
                .satisfies(m -> assertThat(m).contains("unreadable collector command ignored"));
    }

    @Test
    void aCommandForARunningJobIsSkippedWithALog() throws IOException {
        when(jobs.startOnDemand(Job.COLLECT_POST_CLOSE)).thenReturn(Outcome.ALREADY_RUNNING);

        listener.onCommand(command(UUID.randomUUID().toString(), "collect-post-close"));

        assertThat(messages(Level.WARN)).singleElement().satisfies(m -> assertThat(m)
                .contains("collector command skipped, job already running").contains("command=collect-post-close"));
    }

    @Test
    void commandsSentWhileTheCollectorWasOffAreNotReadAfterItStarts() {
        ConsumerSeekCallback callback = mock(ConsumerSeekCallback.class);
        TopicPartition partition = new TopicPartition(Topics.COLLECTOR_COMMAND, 0);

        listener.onPartitionsAssigned(Map.of(partition, 7L), callback);

        verify(callback).seekToEnd(Set.of(partition));
        verify(callback, never()).seekToBeginning(any(Collection.class));
    }

    @Test
    void listensOnTheCommandTopicInItsOwnGroup() throws NoSuchMethodException {
        Method method = CollectorCommandListener.class.getDeclaredMethod("onCommand", String.class);
        KafkaListener annotation = method.getAnnotation(KafkaListener.class);

        assertThat(annotation.topics()).containsExactly(Topics.COLLECTOR_COMMAND);
        assertThat(annotation.groupId()).isEqualTo("collector-command");
    }

    private static String command(String eventId, String name) throws IOException {
        return ContractSchemas.sample("collector.command.sample.json")
                .replace(SAMPLE_EVENT_ID, eventId)
                .replace("\"command\": \"collect\"", "\"command\": \"" + name + "\"");
    }

    private List<String> messages(Level level) {
        return logs.list.stream().filter(e -> e.getLevel() == level).map(ILoggingEvent::getFormattedMessage).toList();
    }
}

package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TickPublisherTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void newsTickIsKeyedByJobTypeAndMatchesTheContract() throws IOException {
        new TickPublisher(kafka, mapper, CLOCK).publishCollectNewsTick();

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-news"), value.capture());
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", value.getValue())).isEmpty();
        assertThat(value.getValue())
                .contains("\"jobType\":\"collect-news\"")
                .contains("\"correlationId\":\"collect-news-2026-10-20T16:00:00+09:00\"");
    }

    @Test
    void timerHasNoFilingsTickBecauseFilingsComeOnlyWithThePreOpenTick() {
        // 공시는 장 시작 전 tick 에서만 받는다. 이 타이머에 주기 작업이 다시 생기면 공시 15분 수집이 되살아난다.
        List<String> timers = Arrays.stream(TickPublisher.class.getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(Scheduled.class))
                .map(Method::getName)
                .sorted()
                .toList();

        assertThat(timers).containsExactly("publishCollectNewsTick", "publishCollectTick");
    }

    @Test
    void collectTickKeepsItsFormat() {
        new TickPublisher(kafka, mapper, CLOCK).publishCollectTick();

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect"), value.capture());
        assertThat(value.getValue()).contains("\"correlationId\":\"collect-2026-10-20T16:00:00+09:00\"");
    }

    @Test
    void timerBeanIsAbsentByDefault() {
        runner().run(context -> assertThat(context).doesNotHaveBean(TickPublisher.class));
    }

    @Test
    void timerBeanIsAbsentWhenDisabled() {
        runner().withPropertyValues("collector.schedule.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(TickPublisher.class));
    }

    @Test
    void timerBeanExistsWhenEnabled() {
        runner().withPropertyValues("collector.schedule.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(TickPublisher.class));
    }

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
                .withBean(KafkaTemplate.class, () -> kafka)
                .withBean(ObjectMapper.class, () -> mapper)
                .withBean(Clock.class, () -> CLOCK)
                .withUserConfiguration(TickPublisher.class);
    }
}

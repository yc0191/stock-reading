package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TickPublisherTest {

    // 화요일 16:00(한국 시간): 시세 시간대 밖
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final QuoteHoursProperties HOURS = new QuoteHoursProperties("09:00", "15:30");

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void newsTickIsKeyedByJobTypeAndMatchesTheContract() throws IOException {
        new TickPublisher(HOURS, kafka, mapper, CLOCK).publishCollectNewsTick();

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
        publishCollectAt("2026-10-20T01:00:00Z");

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect"), value.capture());
        assertThat(value.getValue()).contains("\"correlationId\":\"collect-2026-10-20T10:00:00+09:00\"");
    }

    // 09:00:00, 15:30:00 은 양끝이라 포함하고, 끝 시각은 분 단위로 비교해 15:30:59 까지 포함한다.
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-20T00:00:00Z", "2026-10-20T06:30:00Z", "2026-10-20T06:30:59Z"})
    void collectTickIsPublishedAtTheEdgesOfTheQuoteHours(String instant) {
        publishCollectAt(instant);

        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect"), anyString());
    }

    // 08:59:59, 15:31:00, 화요일 16:00, 토요일 10:00, 일요일 10:00
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-19T23:59:59Z", "2026-10-20T06:31:00Z", "2026-10-20T07:00:00Z",
            "2026-10-24T01:00:00Z", "2026-10-25T01:00:00Z"})
    void collectTickIsNotPublishedOutsideTheQuoteHoursOrOnWeekends(String instant) {
        publishCollectAt(instant);

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    // 뉴스는 시세 시간대와 상관없다: 장 전 08:00, 장 후 16:00, 토요일 10:00 에도 발행한다.
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-19T23:00:00Z", "2026-10-20T07:00:00Z", "2026-10-24T01:00:00Z"})
    void newsTickIsPublishedOutsideTheQuoteHours(String instant) {
        Clock clock = Clock.fixed(Instant.parse(instant), ZoneId.of("Asia/Seoul"));
        new TickPublisher(HOURS, kafka, mapper, clock).publishCollectNewsTick();

        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-news"), anyString());
    }

    @EnableConfigurationProperties(QuoteHoursProperties.class)
    static class Wiring {
    }

    @Test
    void quoteHoursDefaultToNineToHalfPastThreeAndCanBeChangedByEnvironment() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class);

        runner.run(context -> {
            QuoteHoursProperties properties = context.getBean(QuoteHoursProperties.class);
            assertThat(properties.startTime()).hasToString("09:00");
            assertThat(properties.endTime()).hasToString("15:30");
        });
        runner.withPropertyValues("COLLECTOR_QUOTE_START=08:00", "COLLECTOR_QUOTE_END=20:00")
                .run(context -> {
                    QuoteHoursProperties properties = context.getBean(QuoteHoursProperties.class);
                    assertThat(properties.startTime()).hasToString("08:00");
                    assertThat(properties.endTime()).hasToString("20:00");
                });
    }

    @Test
    void rejectsQuoteHoursThatCannotWork() {
        assertThatThrownBy(() -> new QuoteHoursProperties("15:30", "09:00"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new QuoteHoursProperties("9시", "15:30"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void blankQuoteHoursFallBackToTheTradingHours() {
        QuoteHoursProperties properties = new QuoteHoursProperties("", null);

        assertThat(properties.start()).isEqualTo("09:00");
        assertThat(properties.end()).isEqualTo("15:30");
    }

    private void publishCollectAt(String instant) {
        Clock clock = Clock.fixed(Instant.parse(instant), ZoneId.of("Asia/Seoul"));
        new TickPublisher(HOURS, kafka, mapper, clock).publishCollectTick();
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
                .withBean(QuoteHoursProperties.class, () -> HOURS)
                .withBean(KafkaTemplate.class, () -> kafka)
                .withBean(ObjectMapper.class, () -> mapper)
                .withBean(Clock.class, () -> CLOCK)
                .withUserConfiguration(TickPublisher.class);
    }
}

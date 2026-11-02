package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

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

class IntradayTickPublisherTest {

    private static final IntradayProperties DEFAULTS =
            new IntradayProperties(true, "0 */5 9-15 * * MON-FRI", "09:00", "15:30", 15_000);

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

    private void publishAt(String instant) {
        Clock clock = Clock.fixed(Instant.parse(instant), ZoneId.of("Asia/Seoul"));
        new IntradayTickPublisher(DEFAULTS, kafka, mapper, clock).publishIntradayTick();
    }

    @Test
    void publishesAnIntradayTickThatPassesTheTickContract() throws IOException {
        // 2026-10-21(수) 10:05 한국 시간
        publishAt("2026-10-21T01:05:00Z");

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-intraday"), value.capture());
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", value.getValue())).isEmpty();
        assertThat(value.getValue())
                .contains("\"jobType\":\"collect-intraday\"")
                .contains("\"scheduledFor\":\"2026-10-21T10:05:00+09:00\"")
                .contains("\"correlationId\":\"collect-intraday-2026-10-21T10:05:00+09:00\"");
    }

    // 09:00, 15:30 은 양끝이라 포함하고, 타이머가 몇 초 늦게 불린 15:30:02 도 포함한다.
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-21T00:00:00Z", "2026-10-21T06:30:00Z", "2026-10-21T06:30:02Z"})
    void publishesAtTheEdgesOfTheTradingHours(String instant) {
        publishAt(instant);

        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-intraday"), anyString());
    }

    // 08:55, 15:35, 15:55(cron 이 9-15 시라 부르는 마지막 시각), 토요일 10:00, 일요일 10:00
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-20T23:55:00Z", "2026-10-21T06:35:00Z", "2026-10-21T06:55:00Z",
            "2026-10-24T01:00:00Z", "2026-10-25T01:00:00Z"})
    void publishesNothingOutsideTheTradingHoursOrOnWeekends(String instant) {
        publishAt(instant);

        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void cronIsReadFromSettingsAndEvaluatedInKoreanTime() throws NoSuchMethodException {
        Scheduled scheduled = IntradayTickPublisher.class.getDeclaredMethod("publishIntradayTick")
                .getAnnotation(Scheduled.class);

        assertThat(scheduled.cron()).isEqualTo("${collector.intraday.cron}");
        assertThat(scheduled.zone()).isEqualTo("Asia/Seoul");
    }

    @EnableConfigurationProperties(IntradayProperties.class)
    static class Wiring {
    }

    @Test
    void isOffByDefaultAndRunsEveryFiveMinutesFromNineToHalfPastThreeWhenEnabled() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .withBean(ObjectMapper.class, () -> JsonMapper.builder().build())
                .withBean(Clock.class, Clock::systemUTC)
                .withUserConfiguration(Wiring.class, IntradayTickPublisher.class);

        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(IntradayTickPublisher.class);
            IntradayProperties properties = context.getBean(IntradayProperties.class);
            assertThat(properties.enabled()).isFalse();
            assertThat(properties.cron()).isEqualTo("0 */5 9-15 * * MON-FRI");
            assertThat(properties.start()).isEqualTo("09:00");
            assertThat(properties.end()).isEqualTo("15:30");
            assertThat(properties.staggerMs()).isEqualTo(15_000);
        });
        runner.withPropertyValues("COLLECTOR_INTRADAY_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(IntradayTickPublisher.class));
        runner.withPropertyValues("COLLECTOR_INTRADAY_CRON=0 */10 9-15 * * MON-FRI", "COLLECTOR_INTRADAY_START=09:10",
                        "COLLECTOR_INTRADAY_END=15:20", "COLLECTOR_INTRADAY_STAGGER_MS=0")
                .run(context -> {
                    IntradayProperties properties = context.getBean(IntradayProperties.class);
                    assertThat(properties.cron()).isEqualTo("0 */10 9-15 * * MON-FRI");
                    assertThat(properties.startTime()).hasToString("09:10");
                    assertThat(properties.endTime()).hasToString("15:20");
                    assertThat(properties.staggerMs()).isZero();
                });
    }

    @Test
    void rejectsSettingsThatCannotWork() {
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "15:30", "09:00", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "9시", "15:30", 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "09:00", "15:30", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IntradayProperties(false, "c", "09:00", "15:30", 60_001))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void blankHoursFallBackToTheTradingHours() {
        IntradayProperties properties = new IntradayProperties(false, "c", "", null, 0);

        assertThat(properties.start()).isEqualTo("09:00");
        assertThat(properties.end()).isEqualTo("15:30");
    }
}

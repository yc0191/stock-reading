package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class DailyBriefingTickPublisherTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    @SuppressWarnings("unchecked")
    @Test
    void morningCronPublishesOneMorningTickThatPassesTheTickContract() throws IOException {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        Clock clock = Clock.fixed(Instant.parse("2026-10-20T23:45:00Z"), KST);

        new DailyBriefingTickPublisher(kafka, JsonMapper.builder().build(), clock).publishMorningTick();

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(1)).send(anyString(), anyString(), anyString());
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("daily-briefing-morning"), value.capture());
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", value.getValue())).isEmpty();
        assertThat(value.getValue())
                .contains("\"jobType\":\"daily-briefing-morning\"")
                .contains("\"scheduledFor\":\"2026-10-21T08:45:00+09:00\"")
                .contains("\"correlationId\":\"daily-briefing-morning-2026-10-21T08:45:00+09:00\"");
    }

    @SuppressWarnings("unchecked")
    @Test
    void postCloseCronPublishesOnePostCloseTickThatPassesTheTickContract() throws IOException {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        Clock clock = Clock.fixed(Instant.parse("2026-10-21T08:00:00Z"), KST);

        new DailyBriefingTickPublisher(kafka, JsonMapper.builder().build(), clock).publishPostCloseTick();

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(1)).send(anyString(), anyString(), anyString());
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("daily-briefing-post-close"), value.capture());
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", value.getValue())).isEmpty();
        assertThat(value.getValue())
                .contains("\"jobType\":\"daily-briefing-post-close\"")
                .contains("\"scheduledFor\":\"2026-10-21T17:00:00+09:00\"")
                .contains("\"correlationId\":\"daily-briefing-post-close-2026-10-21T17:00:00+09:00\"");
    }

    @Test
    void cronsAreReadFromSettingsAndEvaluatedInKoreanTime() throws NoSuchMethodException {
        Scheduled morning = DailyBriefingTickPublisher.class.getDeclaredMethod("publishMorningTick")
                .getAnnotation(Scheduled.class);
        Scheduled postClose = DailyBriefingTickPublisher.class.getDeclaredMethod("publishPostCloseTick")
                .getAnnotation(Scheduled.class);

        assertThat(morning.cron()).isEqualTo("${collector.daily-briefing.morning-cron}");
        assertThat(morning.zone()).isEqualTo("Asia/Seoul");
        assertThat(postClose.cron()).isEqualTo("${collector.daily-briefing.post-close-cron}");
        assertThat(postClose.zone()).isEqualTo("Asia/Seoul");
    }

    @Test
    void isOffByDefaultAndRunsAtEightFortyFiveAndFiveOnWeekdaysWhenEnabled() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .withBean(ObjectMapper.class, () -> JsonMapper.builder().build())
                .withBean(Clock.class, Clock::systemUTC)
                .withUserConfiguration(DailyBriefingTickPublisher.class);

        runner.run(context -> {
            assertThat(context).doesNotHaveBean(DailyBriefingTickPublisher.class);
            assertThat(context.getEnvironment().getProperty("collector.daily-briefing.morning-cron"))
                    .isEqualTo("0 45 8 * * MON-FRI");
            assertThat(context.getEnvironment().getProperty("collector.daily-briefing.post-close-cron"))
                    .isEqualTo("0 0 17 * * MON-FRI");
        });
        runner.withPropertyValues("COLLECTOR_DAILY_BRIEFING_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(DailyBriefingTickPublisher.class));
        runner.withPropertyValues(
                        "COLLECTOR_DAILY_BRIEFING_MORNING_CRON=0 50 8 * * MON-FRI",
                        "COLLECTOR_DAILY_BRIEFING_POST_CLOSE_CRON=-")
                .run(context -> {
                    assertThat(context.getEnvironment().getProperty("collector.daily-briefing.morning-cron"))
                            .isEqualTo("0 50 8 * * MON-FRI");
                    assertThat(context.getEnvironment().getProperty("collector.daily-briefing.post-close-cron"))
                            .isEqualTo(Scheduled.CRON_DISABLED);
                });
    }

    @Test
    void defaultCronsFireOnWeekdaysOnlyWithoutCheckingMarketHolidays() {
        CronExpression morning = CronExpression.parse("0 45 8 * * MON-FRI");
        CronExpression postClose = CronExpression.parse("0 0 17 * * MON-FRI");

        // 금요일 17:00 뒤 다음 아침 판은 월요일 08:45 다(주말은 건너뜀).
        ZonedDateTime fridayEvening = ZonedDateTime.of(2026, 10, 23, 17, 0, 0, 0, KST);
        assertThat(morning.next(fridayEvening)).isEqualTo(ZonedDateTime.of(2026, 10, 26, 8, 45, 0, 0, KST));
        assertThat(postClose.next(fridayEvening)).isEqualTo(ZonedDateTime.of(2026, 10, 26, 17, 0, 0, 0, KST));
        // 휴장일을 판정하지 않으므로 평일 공휴일(2026-10-09 금, 한글날)에도 발행 시각이 온다.
        ZonedDateTime holidayEve = ZonedDateTime.of(2026, 10, 8, 18, 0, 0, 0, KST);
        assertThat(morning.next(holidayEve)).isEqualTo(ZonedDateTime.of(2026, 10, 9, 8, 45, 0, 0, KST));
        assertThat(postClose.next(holidayEve)).isEqualTo(ZonedDateTime.of(2026, 10, 9, 17, 0, 0, 0, KST));
    }
}

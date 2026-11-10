package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.marketdata.MarketDayPort;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class PreOpenTickPublisherTest {

    private static final MarketDayPort OPEN = () -> true;
    private static final MarketDayPort HOLIDAY = () -> false;

    @SuppressWarnings("unchecked")
    @Test
    void publishesAPreOpenTickThatPassesTheTickContract() throws IOException {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        ObjectMapper mapper = JsonMapper.builder().build();
        Clock clock = Clock.fixed(Instant.parse("2026-10-20T23:30:00Z"), ZoneId.of("Asia/Seoul"));

        new PreOpenTickPublisher(OPEN, kafka, mapper, clock).publishPreOpenTick();

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-pre-open"), value.capture());
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", value.getValue())).isEmpty();
        assertThat(value.getValue())
                .contains("\"jobType\":\"collect-pre-open\"")
                .contains("\"scheduledFor\":\"2026-10-21T08:30:00+09:00\"")
                .contains("\"correlationId\":\"collect-pre-open-2026-10-21T08:30:00+09:00\"");
    }

    @Test
    void cronIsReadFromSettingsAndEvaluatedInKoreanTime() throws NoSuchMethodException {
        Scheduled scheduled = PreOpenTickPublisher.class.getDeclaredMethod("publishPreOpenTick")
                .getAnnotation(Scheduled.class);

        assertThat(scheduled.cron()).isEqualTo("${collector.pre-open.cron}");
        assertThat(scheduled.zone()).isEqualTo("Asia/Seoul");
    }

    @Test
    void isOffByDefaultAndRunsAtEightThirtyOnWeekdaysWhenEnabled() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .withBean(ObjectMapper.class, () -> JsonMapper.builder().build())
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(MarketDayPort.class, () -> OPEN)
                .withUserConfiguration(PreOpenTickPublisher.class);

        runner.run(context -> {
            assertThat(context).doesNotHaveBean(PreOpenTickPublisher.class);
            assertThat(context.getEnvironment().getProperty("collector.pre-open.cron")).isEqualTo("0 30 8 * * MON-FRI");
        });
        runner.withPropertyValues("COLLECTOR_PRE_OPEN_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(PreOpenTickPublisher.class));
        runner.withPropertyValues("COLLECTOR_PRE_OPEN_CRON=0 0 8 * * MON-FRI")
                .run(context -> assertThat(context.getEnvironment().getProperty("collector.pre-open.cron"))
                        .isEqualTo("0 0 8 * * MON-FRI"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void marketHolidayPublishesNoPreOpenTick() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        Clock clock = Clock.fixed(Instant.parse("2026-10-08T23:30:00Z"), ZoneId.of("Asia/Seoul"));

        new PreOpenTickPublisher(HOLIDAY, kafka, JsonMapper.builder().build(), clock).publishPreOpenTick();

        verifyNoInteractions(kafka);
    }
}

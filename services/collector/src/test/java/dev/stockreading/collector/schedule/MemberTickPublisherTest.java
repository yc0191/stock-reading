package dev.stockreading.collector.schedule;

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

class MemberTickPublisherTest {

    @SuppressWarnings("unchecked")
    @Test
    void publishesAMembersTickThatPassesTheTickContract() throws IOException {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        ObjectMapper mapper = JsonMapper.builder().build();
        Clock clock = Clock.fixed(Instant.parse("2026-10-20T23:00:00Z"), ZoneId.of("Asia/Seoul"));

        new MemberTickPublisher(() -> true, kafka, mapper, clock).publishMembersTick();

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-members"), value.capture());
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", value.getValue())).isEmpty();
        assertThat(value.getValue())
                .contains("\"jobType\":\"collect-members\"")
                .contains("\"scheduledFor\":\"2026-10-21T08:00:00+09:00\"");
    }

    @SuppressWarnings("unchecked")
    @Test
    void doesNotPublishOnAMarketHoliday() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        Clock clock = Clock.fixed(Instant.parse("2026-10-20T23:00:00Z"), ZoneId.of("Asia/Seoul"));

        new MemberTickPublisher(() -> false, kafka, JsonMapper.builder().build(), clock).publishMembersTick();

        verifyNoInteractions(kafka);
    }

    @Test
    void cronIsReadFromSettingsAndEvaluatedInKoreanTime() throws NoSuchMethodException {
        Scheduled scheduled = MemberTickPublisher.class.getDeclaredMethod("publishMembersTick")
                .getAnnotation(Scheduled.class);

        assertThat(scheduled.cron()).isEqualTo("${collector.members.cron}");
        assertThat(scheduled.zone()).isEqualTo("Asia/Seoul");
    }

    @Test
    void isOffByDefaultAndRunsAtEightOnWeekdaysWhenEnabled() {
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .withBean(ObjectMapper.class, () -> JsonMapper.builder().build())
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(MarketDayPort.class, () -> () -> true)
                .withUserConfiguration(MemberTickPublisher.class);

        runner.run(context -> {
            assertThat(context).doesNotHaveBean(MemberTickPublisher.class);
            assertThat(context.getEnvironment().getProperty("collector.members.cron")).isEqualTo("0 0 8 * * MON-FRI");
        });
        runner.withPropertyValues("COLLECTOR_MEMBERS_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(MemberTickPublisher.class));
    }
}

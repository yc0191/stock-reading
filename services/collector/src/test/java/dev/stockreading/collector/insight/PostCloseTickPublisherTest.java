package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
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
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.marketdata.MarketDayPort;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class PostCloseTickPublisherTest {

    private static final MarketDayPort OPEN = () -> true;
    private static final MarketDayPort HOLIDAY = () -> false;

    @SuppressWarnings("unchecked")
    @Test
    void publishesAPostCloseTickThatPassesTheTickContract() throws IOException {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        ObjectMapper mapper = JsonMapper.builder().build();
        Clock clock = Clock.fixed(Instant.parse("2026-10-20T07:30:00Z"), ZoneId.of("Asia/Seoul"));

        new PostCloseTickPublisher(OPEN, kafka, mapper, clock).publishPostCloseTick();

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-post-close"), value.capture());
        assertThat(ContractSchemas.validate("schedule.tick.v1.schema.json", value.getValue())).isEmpty();
        assertThat(value.getValue())
                .contains("\"jobType\":\"collect-post-close\"")
                .contains("\"scheduledFor\":\"2026-10-20T16:30:00+09:00\"")
                .contains("\"correlationId\":\"collect-post-close-2026-10-20T16:30:00+09:00\"");
    }

    @Test
    void cronIsReadFromSettingsAndEvaluatedInKoreanTime() throws NoSuchMethodException {
        Scheduled scheduled = PostCloseTickPublisher.class.getDeclaredMethod("publishPostCloseTick")
                .getAnnotation(Scheduled.class);

        assertThat(scheduled.cron()).isEqualTo("${collector.insight.schedule.cron}");
        assertThat(scheduled.zone()).isEqualTo("Asia/Seoul");
    }

    @Test
    void isNotCreatedUnlessExplicitlyEnabled() {
        var runner = new ApplicationContextRunner()
                .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .withBean(ObjectMapper.class, () -> JsonMapper.builder().build())
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(MarketDayPort.class, () -> OPEN)
                .withUserConfiguration(PostCloseTickPublisher.class);

        runner.run(context -> assertThat(context).doesNotHaveBean(PostCloseTickPublisher.class));
        runner.withPropertyValues("collector.insight.schedule.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(PostCloseTickPublisher.class));
        runner.withPropertyValues("collector.insight.schedule.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(PostCloseTickPublisher.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    void marketHolidayPublishesNoPostCloseTick() {
        KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
        Clock clock = Clock.fixed(Instant.parse("2026-10-09T07:30:00Z"), ZoneId.of("Asia/Seoul"));

        new PostCloseTickPublisher(HOLIDAY, kafka, JsonMapper.builder().build(), clock).publishPostCloseTick();

        verifyNoInteractions(kafka);
    }
}

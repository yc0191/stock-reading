package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.MDC;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.marketdata.MarketDayPort;
import dev.stockreading.collector.trace.CorrelationId;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class DailyCollectionPublisherTest {

    private static final MarketDayPort OPEN = () -> true;
    private static final MarketDayPort HOLIDAY = () -> false;
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final String SCHEMA = "daily.collection.completed.v1.schema.json";
    // 2026-10-21(수) 09:10, 16:31:05 한국 시간
    private static final Clock MORNING = Clock.fixed(Instant.parse("2026-10-21T00:10:00Z"), KST);
    private static final Clock POST_CLOSE = Clock.fixed(Instant.parse("2026-10-21T07:31:05Z"), KST);

    private final ObjectMapper mapper = JsonMapper.builder().build();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

    private DailyCollectionPublisher publisher(MarketDayPort marketDay, Clock clock) {
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(new CompletableFuture<>());
        return new DailyCollectionPublisher(marketDay, kafka, mapper, clock);
    }

    private JsonNode sentOne(String edition) throws IOException {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(1)).send(anyString(), anyString(), anyString());
        verify(kafka).send(eq(Topics.DAILY_COLLECTION_COMPLETED), eq(edition), value.capture());
        assertThat(ContractSchemas.validate(SCHEMA, value.getValue())).isEmpty();
        return mapper.readTree(value.getValue());
    }

    private static List<String> missing(JsonNode event) {
        List<String> out = new ArrayList<>();
        event.get("payload").get("missingSources").forEach(n -> out.add(n.asString()));
        return out;
    }

    @Test
    void morningNoticeCarriesWhatTodaysPreOpenCollectionMissed() throws IOException {
        DailyCollectionPublisher publisher = publisher(OPEN, MORNING);
        publisher.recordPreOpen(List.of("overseas-daily-price"));

        publisher.publishMorning();

        JsonNode event = sentOne("morning");
        assertThat(event.get("eventType").asString()).isEqualTo("daily.collection.completed");
        assertThat(event.get("correlationId").asString()).isEqualTo("daily-morning-2026-10-21T09:10:00+09:00");
        assertThat(event.get("payload").get("status").asString()).isEqualTo("collected");
        assertThat(event.get("payload").get("scheduledFor").asString()).isEqualTo("2026-10-21T09:10:00+09:00");
        assertThat(missing(event)).containsExactly("overseas-daily-price");
    }

    @Test
    void morningNoticeSaysEveryPreOpenSourceIsMissingWhenThePreOpenCollectionDidNotRun() throws IOException {
        publisher(OPEN, MORNING).publishMorning();

        assertThat(missing(sentOne("morning"))).containsExactly("world-index", "overseas-daily-price", "sec-filings");
    }

    @Test
    void aPreOpenResultFromAnotherDayIsNotUsed() throws IOException {
        // 같은 발행기가 어제 결과를 들고 있다가 오늘 판 시각을 맞는 경우: 한국 날짜가 다르면 돌지 않은 것으로 본다.
        MutableClock clock = new MutableClock(Instant.parse("2026-10-20T00:00:00Z"));
        DailyCollectionPublisher publisher = publisher(OPEN, clock);
        publisher.recordPreOpen(List.of());
        clock.now = Instant.parse("2026-10-21T00:10:00Z");

        publisher.publishMorning();

        assertThat(missing(sentOne("morning"))).containsExactly("world-index", "overseas-daily-price", "sec-filings");
    }

    @Test
    void postCloseNoticeContinuesTheCollectionsCorrelationIdAndUsesTheFinishTimeAsTheEditionTime() throws IOException {
        publisher(OPEN, POST_CLOSE).publishPostClose("collect-post-close-2026-10-21T16:30:00+09:00",
                List.of("world-index", "market-summary"));

        JsonNode event = sentOne("post-close");
        assertThat(event.get("correlationId").asString()).isEqualTo("collect-post-close-2026-10-21T16:30:00+09:00");
        assertThat(event.get("payload").get("status").asString()).isEqualTo("collected");
        assertThat(event.get("payload").get("scheduledFor").asString()).isEqualTo("2026-10-21T16:31:05+09:00");
        assertThat(missing(event)).containsExactly("world-index", "market-summary");
    }

    @Test
    void holidaySendsMarketClosedForBothEditionsAtTheirTimes() throws IOException {
        publisher(HOLIDAY, MORNING).publishMorning();
        JsonNode morning = sentOne("morning");
        assertThat(morning.get("payload").get("status").asString()).isEqualTo("market-closed");
        assertThat(missing(morning)).isEmpty();

        KafkaTemplate<String, String> other = kafkaReturningFutures();
        new DailyCollectionPublisher(HOLIDAY, other, mapper, POST_CLOSE).publishPostCloseHoliday();
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(other).send(eq(Topics.DAILY_COLLECTION_COMPLETED), eq("post-close"), value.capture());
        assertThat(ContractSchemas.validate(SCHEMA, value.getValue())).isEmpty();
        assertThat(value.getValue()).contains("\"status\":\"market-closed\"");
    }

    @Test
    void onABusinessDayThePostCloseTimeSendsNothingBecauseTheCollectionSendsItWhenItEnds() {
        new DailyCollectionPublisher(OPEN, kafka, mapper, POST_CLOSE).publishPostCloseHoliday();

        verifyNoInteractions(kafka);
    }

    @Test
    void aFailedSendDoesNotStopTheCaller() {
        when(kafka.send(anyString(), anyString(), anyString())).thenThrow(new IllegalStateException("broker down"));
        DailyCollectionPublisher publisher = new DailyCollectionPublisher(OPEN, kafka, mapper, POST_CLOSE);

        assertThatCode(() -> publisher.publishPostClose("corr", List.of())).doesNotThrowAnyException();
    }

    @Test
    void linesPrintedAtTheEditionTimeCarryTheNoticesCorrelationId() {
        List<String> seen = new ArrayList<>();
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            seen.add(MDC.get(CorrelationId.KEY));
            return new CompletableFuture<>();
        });

        new DailyCollectionPublisher(() -> {
            seen.add(MDC.get(CorrelationId.KEY));
            return false;
        }, kafka, mapper, MORNING).publishPostCloseHoliday();

        String id = "daily-post-close-2026-10-21T09:10:00+09:00";
        assertThat(seen).containsExactly(id, id);
        assertThat(MDC.get(CorrelationId.KEY)).isNull();
    }

    @Test
    void cronsAreReadFromSettingsAndEvaluatedInKoreanTime() throws NoSuchMethodException {
        Scheduled morning = DailyCollectionPublisher.class.getDeclaredMethod("publishMorning")
                .getAnnotation(Scheduled.class);
        Scheduled postClose = DailyCollectionPublisher.class.getDeclaredMethod("publishPostCloseHoliday")
                .getAnnotation(Scheduled.class);

        assertThat(morning.cron()).isEqualTo("${collector.daily-briefing.morning-cron}");
        assertThat(morning.zone()).isEqualTo("Asia/Seoul");
        assertThat(postClose.cron()).isEqualTo("${collector.daily-briefing.post-close-cron}");
        assertThat(postClose.zone()).isEqualTo("Asia/Seoul");
    }

    @Test
    void isOffByDefaultAndRunsAtNineTenAndAtThePostCloseCollectionTimeWhenEnabled() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withBean(KafkaTemplate.class, () -> mock(KafkaTemplate.class))
                .withBean(ObjectMapper.class, () -> JsonMapper.builder().build())
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(MarketDayPort.class, () -> OPEN)
                .withUserConfiguration(DailyCollectionPublisher.class);

        runner.run(context -> {
            assertThat(context).doesNotHaveBean(DailyCollectionPublisher.class);
            assertThat(context.getEnvironment().getProperty("collector.daily-briefing.morning-cron"))
                    .isEqualTo("0 10 9 * * MON-FRI");
            // 휴장 알림 시각은 마감 후 수집 시각과 같다.
            assertThat(context.getEnvironment().getProperty("collector.daily-briefing.post-close-cron"))
                    .isEqualTo(context.getEnvironment().getProperty("collector.insight.schedule.cron"));
        });
        runner.withPropertyValues("COLLECTOR_DAILY_BRIEFING_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(DailyCollectionPublisher.class));
        runner.withPropertyValues("COLLECTOR_DAILY_BRIEFING_POST_CLOSE_CRON=-")
                .run(context -> assertThat(context.getEnvironment()
                        .getProperty("collector.daily-briefing.post-close-cron")).isEqualTo(Scheduled.CRON_DISABLED));
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafkaReturningFutures() {
        KafkaTemplate<String, String> template = mock(KafkaTemplate.class);
        when(template.send(anyString(), anyString(), anyString())).thenReturn(new CompletableFuture<>());
        return template;
    }

    private static final class MutableClock extends Clock {
        Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public ZoneId getZone() {
            return KST;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}

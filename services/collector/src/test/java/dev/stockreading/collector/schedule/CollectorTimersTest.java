package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MDC;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.scheduling.annotation.Scheduled;

import dev.stockreading.collector.insight.IntradayProperties;
import dev.stockreading.collector.marketdata.MarketDayPort;
import dev.stockreading.collector.schedule.CollectorJobs.Job;
import dev.stockreading.collector.trace.CorrelationId;

/** 수집기 타이머가 작업마다 정한 일정·시간대에만 실행 입구를 부르는지 본다. 작업 자체는 부르지 않는다(mock). */
class CollectorTimersTest {

    private static final QuoteHoursProperties QUOTE_HOURS = new QuoteHoursProperties("09:00", "15:30");
    private static final IntradayProperties INTRADAY =
            new IntradayProperties(true, "0 */5 9-15 * * MON-FRI", "09:00", "15:30", 15_000);

    private static final MarketDayPort OPEN = () -> true;
    private static final MarketDayPort HOLIDAY = () -> false;

    private final CollectorJobs jobs = mock(CollectorJobs.class);

    private static Clock at(String instant) {
        return Clock.fixed(Instant.parse(instant), ZoneId.of("Asia/Seoul"));
    }

    // 09:00:00, 15:30:00 은 양끝이라 포함하고, 끝 시각은 분 단위로 비교해 15:30:59 까지 포함한다.
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-20T00:00:00Z", "2026-10-20T03:00:00Z", "2026-10-20T06:30:00Z", "2026-10-20T06:30:59Z"})
    void quotesAreCollectedInsideTheWeekdayQuoteHoursWithoutWaitingForTheFetch(String instant) {
        new CollectTimer(jobs, QUOTE_HOURS, OPEN, at(instant)).collect();

        // 타이머 스레드에서 조회하지 않고 작업 스레드로 넘긴다(start). 그래야 밀린 회차가 몰아서 돌지 않는다.
        verify(jobs).start(Job.COLLECT);
        verify(jobs, never()).run(any());
    }

    // 08:59:59, 15:31:00, 화요일 16:00, 토요일 10:00, 일요일 10:00
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-19T23:59:59Z", "2026-10-20T06:31:00Z", "2026-10-20T07:00:00Z",
            "2026-10-24T01:00:00Z", "2026-10-25T01:00:00Z"})
    void quotesAreNotCollectedOutsideTheQuoteHoursOrOnWeekends(String instant) {
        new CollectTimer(jobs, QUOTE_HOURS, OPEN, at(instant)).collect();

        verify(jobs, never()).start(any());
        verify(jobs, never()).run(any());
    }

    // 뉴스는 시세 시간대와 상관없다: 장 전 08:00, 장 후 16:00, 토요일 10:00 에도 수집한다.
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-19T23:00:00Z", "2026-10-20T07:00:00Z", "2026-10-24T01:00:00Z"})
    void newsIsCollectedOutsideTheQuoteHours(String instant) {
        new CollectTimer(jobs, QUOTE_HOURS, OPEN, at(instant)).collectNews();

        verify(jobs).run(Job.COLLECT_NEWS);
    }

    // 09:00, 15:30 은 양끝이라 포함하고, 타이머가 몇 초 늦게 불린 15:30:02 도 포함한다.
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-21T00:00:00Z", "2026-10-21T01:05:00Z", "2026-10-21T06:30:00Z", "2026-10-21T06:30:02Z"})
    void intradayIsCollectedAtTheEdgesOfTheTradingHours(String instant) {
        new IntradayTimer(jobs, INTRADAY, OPEN, at(instant)).collectIntraday();

        verify(jobs).run(Job.COLLECT_INTRADAY);
    }

    // 08:55, 15:35, 15:55(cron 이 9-15 시라 부르는 마지막 시각), 토요일 10:00, 일요일 10:00
    @ParameterizedTest
    @ValueSource(strings = {"2026-10-20T23:55:00Z", "2026-10-21T06:35:00Z", "2026-10-21T06:55:00Z",
            "2026-10-24T01:00:00Z", "2026-10-25T01:00:00Z"})
    void intradayIsNotCollectedOutsideTheTradingHoursOrOnWeekends(String instant) {
        new IntradayTimer(jobs, INTRADAY, OPEN, at(instant)).collectIntraday();

        verify(jobs, never()).run(any());
    }

    @Test
    void preOpenPostCloseAndMembersRunTheirJobs() {
        new PreOpenTimer(jobs, OPEN).collectPreOpen();
        new PostCloseTimer(jobs, OPEN).collectPostClose();
        new MembersTimer(jobs, OPEN).collectMembers();

        verify(jobs).run(Job.COLLECT_PRE_OPEN);
        verify(jobs).run(Job.COLLECT_POST_CLOSE);
        verify(jobs).run(Job.COLLECT_ETF_MEMBERS);
    }

    @Test
    void marketHolidayStopsQuotePreOpenIntradayPostCloseAndMembersButKeepsNews() {
        // 2026-10-20(화) 10:00 한국 시간, 시세·장중 시간대 안이지만 휴장일로 판정된 경우
        Clock clock = at("2026-10-20T01:00:00Z");
        CollectTimer collect = new CollectTimer(jobs, QUOTE_HOURS, HOLIDAY, clock);

        collect.collect();
        new PreOpenTimer(jobs, HOLIDAY).collectPreOpen();
        new IntradayTimer(jobs, INTRADAY, HOLIDAY, clock).collectIntraday();
        new PostCloseTimer(jobs, HOLIDAY).collectPostClose();
        new MembersTimer(jobs, HOLIDAY).collectMembers();
        // 로그 문맥에 둘 correlationId 는 묻지만 작업은 부르지 않는다.
        verifyNoJobRuns();

        collect.collectNews();
        verify(jobs).run(Job.COLLECT_NEWS);
    }

    @Test
    void marketDayIsNotAskedOutsideTheHours() {
        // 시간대 밖(16:00)에서는 장 운영 상태 조회를 부르지 않는다. 하루 조회 횟수를 줄이기 위해서다.
        MarketDayPort mustNotBeAsked = () -> {
            throw new AssertionError("시간대 밖에서는 장 운영 상태를 묻지 않아야 합니다");
        };

        new CollectTimer(jobs, QUOTE_HOURS, mustNotBeAsked, at("2026-10-20T07:00:00Z")).collect();
        new IntradayTimer(jobs, INTRADAY, mustNotBeAsked, at("2026-10-20T07:00:00Z")).collectIntraday();

        verifyNoJobRuns();
    }

    private void verifyNoJobRuns() {
        verify(jobs, never()).run(any());
        verify(jobs, never()).start(any());
        verify(jobs, never()).startOnDemand(any());
    }

    @Test
    void linesTheTimersPrintBeforeRunningAJobCarryThatRunsCorrelationId() {
        // 휴장일 건너뜀처럼 타이머가 직접 찍는 줄도 그 실행의 correlationId 로 찾을 수 있어야 한다.
        Clock clock = at("2026-10-20T01:00:00Z");
        List<String> seen = new ArrayList<>();
        MarketDayPort recording = () -> {
            seen.add(MDC.get(CorrelationId.KEY));
            return false;
        };
        for (Job job : List.of(Job.COLLECT, Job.COLLECT_PRE_OPEN, Job.COLLECT_INTRADAY, Job.COLLECT_POST_CLOSE,
                Job.COLLECT_ETF_MEMBERS)) {
            when(jobs.correlationId(job)).thenReturn("id-" + job.jobType());
        }

        new CollectTimer(jobs, QUOTE_HOURS, recording, clock).collect();
        new PreOpenTimer(jobs, recording).collectPreOpen();
        new IntradayTimer(jobs, INTRADAY, recording, clock).collectIntraday();
        new PostCloseTimer(jobs, recording).collectPostClose();
        new MembersTimer(jobs, recording).collectMembers();

        assertThat(seen).containsExactly("id-collect", "id-collect-pre-open", "id-collect-intraday", "id-collect-post-close",
                "id-collect-etf-members");
        assertThat(MDC.get(CorrelationId.KEY)).isNull();
    }

    @Test
    void schedulesAreReadFromSettingsAndCronsAreEvaluatedInKoreanTime() throws NoSuchMethodException {
        assertThat(scheduled(CollectTimer.class, "collect").fixedRateString()).isEqualTo("${collector.tick-interval-ms}");
        assertThat(scheduled(CollectTimer.class, "collectNews").fixedRateString())
                .isEqualTo("${collector.schedule.news-interval-ms}");
        assertThat(scheduled(PreOpenTimer.class, "collectPreOpen").cron()).isEqualTo("${collector.pre-open.cron}");
        assertThat(scheduled(IntradayTimer.class, "collectIntraday").cron()).isEqualTo("${collector.intraday.cron}");
        assertThat(scheduled(PostCloseTimer.class, "collectPostClose").cron())
                .isEqualTo("${collector.insight.schedule.cron}");
        assertThat(scheduled(MembersTimer.class, "collectMembers").cron()).isEqualTo("${collector.members.cron}");
        for (Scheduled cron : List.of(scheduled(PreOpenTimer.class, "collectPreOpen"),
                scheduled(IntradayTimer.class, "collectIntraday"), scheduled(PostCloseTimer.class, "collectPostClose"),
                scheduled(MembersTimer.class, "collectMembers"))) {
            assertThat(cron.zone()).isEqualTo("Asia/Seoul");
        }
    }

    @Test
    void timersHaveOnlyTheSixCollectionSchedules() {
        // 공시는 장 시작 전 회차에서만 받는다. 타이머에 다른 주기 작업이 생기면 예전 15분 공시 수집 같은 일이 되살아난다.
        List<String> timers = Stream.of(CollectTimer.class, PreOpenTimer.class, IntradayTimer.class, PostCloseTimer.class,
                        MembersTimer.class)
                .flatMap(type -> Arrays.stream(type.getDeclaredMethods()))
                .filter(method -> method.isAnnotationPresent(Scheduled.class))
                .map(Method::getName)
                .sorted()
                .toList();

        assertThat(timers).containsExactly("collect", "collectIntraday", "collectMembers", "collectNews", "collectPostClose",
                "collectPreOpen");
    }

    private static Scheduled scheduled(Class<?> type, String method) throws NoSuchMethodException {
        return type.getDeclaredMethod(method).getAnnotation(Scheduled.class);
    }

    @EnableConfigurationProperties({QuoteHoursProperties.class, IntradayProperties.class})
    static class Wiring {
    }

    private ApplicationContextRunner yamlRunner() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withBean(CollectorJobs.class, () -> jobs)
                .withBean(Clock.class, Clock::systemUTC)
                .withBean(MarketDayPort.class, () -> OPEN)
                .withUserConfiguration(Wiring.class, CollectTimer.class, PreOpenTimer.class, IntradayTimer.class,
                        PostCloseTimer.class, MembersTimer.class);
    }

    @Test
    void everyTimerIsOffByDefaultWithTheDocumentedSchedules() {
        yamlRunner().run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(CollectTimer.class);
            assertThat(context).doesNotHaveBean(PreOpenTimer.class);
            assertThat(context).doesNotHaveBean(IntradayTimer.class);
            assertThat(context).doesNotHaveBean(PostCloseTimer.class);
            assertThat(context).doesNotHaveBean(MembersTimer.class);
            var env = context.getEnvironment();
            assertThat(env.getProperty("collector.tick-interval-ms")).isEqualTo("1000");
            assertThat(env.getProperty("collector.schedule.news-interval-ms")).isEqualTo("900000");
            assertThat(env.getProperty("collector.pre-open.cron")).isEqualTo("0 30 8 * * MON-FRI");
            assertThat(env.getProperty("collector.intraday.cron")).isEqualTo("0 */5 9-15 * * MON-FRI");
            assertThat(env.getProperty("collector.insight.schedule.cron")).isEqualTo("0 30 16 * * MON-FRI");
            assertThat(env.getProperty("collector.members.cron")).isEqualTo("0 0 8 * * MON-FRI");
            QuoteHoursProperties quoteHours = context.getBean(QuoteHoursProperties.class);
            assertThat(quoteHours.startTime()).hasToString("09:00");
            assertThat(quoteHours.endTime()).hasToString("15:30");
        });
    }

    @Test
    void eachTimerIsTurnedOnByItsOwnEnvironmentVariable() {
        yamlRunner().withPropertyValues("COLLECTOR_SCHEDULE_ENABLED=true").run(context -> {
            assertThat(context).hasSingleBean(CollectTimer.class);
            assertThat(context).doesNotHaveBean(PreOpenTimer.class);
        });
        yamlRunner().withPropertyValues("COLLECTOR_PRE_OPEN_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(PreOpenTimer.class));
        yamlRunner().withPropertyValues("COLLECTOR_INTRADAY_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(IntradayTimer.class));
        yamlRunner().withPropertyValues("COLLECTOR_INSIGHT_SCHEDULE_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(PostCloseTimer.class));
        yamlRunner().withPropertyValues("COLLECTOR_MEMBERS_ENABLED=true")
                .run(context -> assertThat(context).hasSingleBean(MembersTimer.class));
        yamlRunner().withPropertyValues("COLLECTOR_SCHEDULE_ENABLED=false", "COLLECTOR_INSIGHT_SCHEDULE_ENABLED=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(CollectTimer.class);
                    assertThat(context).doesNotHaveBean(PostCloseTimer.class);
                });
    }

    @Test
    void schedulesAndQuoteHoursCanBeChangedByEnvironment() {
        yamlRunner().withPropertyValues("COLLECTOR_TICK_INTERVAL_MS=2000", "COLLECTOR_PRE_OPEN_CRON=0 0 8 * * MON-FRI",
                        "COLLECTOR_INSIGHT_CRON=0 0 17 * * MON-FRI", "COLLECTOR_QUOTE_START=08:00", "COLLECTOR_QUOTE_END=20:00")
                .run(context -> {
                    var env = context.getEnvironment();
                    assertThat(env.getProperty("collector.tick-interval-ms")).isEqualTo("2000");
                    assertThat(env.getProperty("collector.pre-open.cron")).isEqualTo("0 0 8 * * MON-FRI");
                    assertThat(env.getProperty("collector.insight.schedule.cron")).isEqualTo("0 0 17 * * MON-FRI");
                    QuoteHoursProperties quoteHours = context.getBean(QuoteHoursProperties.class);
                    assertThat(quoteHours.startTime()).hasToString("08:00");
                    assertThat(quoteHours.endTime()).hasToString("20:00");
                });
    }

    @Test
    void schedulerHasMoreThanOneThreadSoLongJobsDoNotStopTheOneSecondTimer() {
        yamlRunner().run(context -> {
            Integer size = context.getEnvironment().getProperty("spring.task.scheduling.pool.size", Integer.class);
            // 오래 걸리는 작업(뉴스·장 시작 전·장중·마감 후) 넷이 겹쳐도 1초 타이머와 다른 tick 발행기가 돌 자리가 남아야 한다.
            assertThat(size).isGreaterThanOrEqualTo(6);
        });
    }

    @Test
    void maxTickAgeSettingIsGone() {
        yamlRunner().run(context -> assertThat(context.getEnvironment().getProperty("collector.max-tick-age-ms")).isNull());
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
}

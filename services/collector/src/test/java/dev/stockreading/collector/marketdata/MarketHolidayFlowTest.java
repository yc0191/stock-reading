package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.Topics;
import dev.stockreading.collector.insight.IntradayProperties;
import dev.stockreading.collector.schedule.CollectorJobs;
import dev.stockreading.collector.schedule.CollectorJobs.Job;
import dev.stockreading.collector.schedule.QuoteHoursProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 리허설: 가짜 시세 서버의 장 운영 상태 응답 → 휴장일 판정 → 수집기 타이머 4곳과 데일리 판 tick 발행기 → 가짜 실행 입구·가짜 Kafka 까지 끝까지 돌린다.
 * 타이머와 발행기는 다른 패키지의 package-private 클래스라 리플렉션으로 만든다.
 */
class MarketHolidayFlowTest {

    private static final String SCHEDULE = "dev.stockreading.collector.schedule.";
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    // 2026-11-10(화) 10:00 한국 시간: 시세 시간대와 장중 시간대 안
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-10T01:00:00Z"), KST);

    private FakeBrokerServer server;
    private MarketDayPort marketDay;
    private final ObjectMapper mapper = JsonMapper.builder().build();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final CollectorJobs jobs = mock(CollectorJobs.class);

    @BeforeEach
    void start() throws Exception {
        server = new FakeBrokerServer();
        BrokerClient client = new BrokerClient(new BrokerProperties(server.baseUrl(), "ak", "as", 5, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS), mapper, CLOCK, CallRateLimiter.perSecond(10));
        marketDay = new BrokerMarketDay(client, CallRateLimiter.perSecond(10), FakeBrokerServer.MARKET_STATUS, mapper, CLOCK);
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private Object publisher(String className, Object... args) throws Exception {
        Constructor<?> constructor = Class.forName(className).getDeclaredConstructors()[0];
        constructor.setAccessible(true);
        return constructor.newInstance(args);
    }

    private void fire(Object publisher, String method) throws Exception {
        Method m = publisher.getClass().getDeclaredMethod(method);
        m.setAccessible(true);
        m.invoke(publisher);
    }

    private void fireEverySchedule() throws Exception {
        Object collect = publisher(SCHEDULE + "CollectTimer", jobs, new QuoteHoursProperties("09:00", "15:30"), marketDay, CLOCK);
        fire(collect, "collect");
        fire(collect, "collectNews");
        fire(publisher(SCHEDULE + "PreOpenTimer", jobs, marketDay), "collectPreOpen");
        fire(publisher(SCHEDULE + "IntradayTimer", jobs,
                new IntradayProperties(true, "0 */5 9-15 * * MON-FRI", "09:00", "15:30", 15_000), marketDay, CLOCK),
                "collectIntraday");
        fire(publisher(SCHEDULE + "PostCloseTimer", jobs, marketDay), "collectPostClose");
        fire(publisher(SCHEDULE + "MemberTickPublisher", marketDay, kafka, mapper, CLOCK), "publishMembersTick");
        Object daily = publisher(SCHEDULE + "DailyBriefingTickPublisher", marketDay, kafka, mapper, CLOCK);
        fire(daily, "publishMorningTick");
        fire(daily, "publishPostCloseTick");
    }

    private void verifyEveryJobAndTickRan() {
        verify(jobs).start(Job.COLLECT);
        for (Job job : new Job[] {Job.COLLECT_NEWS, Job.COLLECT_PRE_OPEN, Job.COLLECT_INTRADAY, Job.COLLECT_POST_CLOSE}) {
            verify(jobs).run(job);
        }
        for (String jobType : new String[] {"collect-members", "daily-briefing-morning", "daily-briefing-post-close"}) {
            verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq(jobType), anyString());
        }
    }

    @Test
    void holidayStopsEveryMarketJobAndTickIncludingTheMembersTickAndKeepsTheNews() throws Exception {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261109");

        fireEverySchedule();

        verify(jobs).run(Job.COLLECT_NEWS);
        verify(jobs, never()).start(any());
        for (Job job : new Job[] {Job.COLLECT_PRE_OPEN, Job.COLLECT_INTRADAY, Job.COLLECT_POST_CLOSE}) {
            verify(jobs, never()).run(job);
        }
        for (String jobType : new String[] {"collect-members", "daily-briefing-morning", "daily-briefing-post-close"}) {
            verify(kafka, never()).send(eq(Topics.SCHEDULE_TICK), eq(jobType), anyString());
        }
        // 여러 곳이 물었지만 서버 조회는 하루 한 번이다.
        assertThat(server.marketStatusCalls).hasValue(1);
    }

    @Test
    void businessDayRunsEveryJobAndTick() throws Exception {
        fireEverySchedule();

        verifyEveryJobAndTickRan();
    }

    @Test
    void failingStatusLookupRunsAsOnAnOrdinaryBusinessDay() throws Exception {
        server.marketStatusStatus = 500;

        fireEverySchedule();

        verifyEveryJobAndTickRan();
        // 세 번 다시 시도한 뒤 쿨다운 동안은 더 묻지 않는다.
        assertThat(server.marketStatusCalls).hasValue(BrokerMarketDay.MAX_ATTEMPTS);
    }
}

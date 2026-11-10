package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
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
import dev.stockreading.collector.schedule.QuoteHoursProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 리허설: 가짜 시세 서버의 장 운영 상태 응답 → 휴장일 판정 → tick 발행기 6곳 → 가짜 Kafka 까지 끝까지 돌린다.
 * 발행기는 다른 패키지의 package-private 클래스라 리플렉션으로 만든다.
 */
class MarketHolidayFlowTest {

    private static final String SCHEDULE = "dev.stockreading.collector.schedule.";
    private static final String INSIGHT = "dev.stockreading.collector.insight.";
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    // 2026-11-10(화) 10:00 한국 시간: 시세 시간대와 장중 시간대 안
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-11-10T01:00:00Z"), KST);

    private FakeBrokerServer server;
    private MarketDayPort marketDay;
    private final ObjectMapper mapper = JsonMapper.builder().build();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);

    @BeforeEach
    void start() throws Exception {
        server = new FakeBrokerServer();
        BrokerClient client = new BrokerClient(new BrokerProperties(server.baseUrl(), "ak", "as", 5, 10), mapper, CLOCK, CallRateLimiter.perSecond(10));
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
        fire(publisher(SCHEDULE + "TickPublisher", new QuoteHoursProperties("09:00", "15:30"), marketDay, kafka, mapper, CLOCK),
                "publishCollectTick");
        fire(publisher(SCHEDULE + "TickPublisher", new QuoteHoursProperties("09:00", "15:30"), marketDay, kafka, mapper, CLOCK),
                "publishCollectNewsTick");
        fire(publisher(INSIGHT + "PreOpenTickPublisher", marketDay, kafka, mapper, CLOCK), "publishPreOpenTick");
        fire(publisher(INSIGHT + "IntradayTickPublisher",
                new IntradayProperties(true, "0 */5 9-15 * * MON-FRI", "09:00", "15:30", 15_000),
                marketDay, kafka, mapper, CLOCK), "publishIntradayTick");
        fire(publisher(INSIGHT + "PostCloseTickPublisher", marketDay, kafka, mapper, CLOCK), "publishPostCloseTick");
        Object daily = publisher(SCHEDULE + "DailyBriefingTickPublisher", marketDay, kafka, mapper, CLOCK);
        fire(daily, "publishMorningTick");
        fire(daily, "publishPostCloseTick");
    }

    @Test
    void holidayStopsEveryMarketTickAndKeepsTheNewsTick() throws Exception {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261109");

        fireEverySchedule();

        verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq("collect-news"), anyString());
        for (String jobType : new String[] {"collect", "collect-pre-open", "collect-intraday", "collect-post-close",
                "daily-briefing-morning", "daily-briefing-post-close"}) {
            verify(kafka, never()).send(eq(Topics.SCHEDULE_TICK), eq(jobType), anyString());
        }
        // 여섯 곳이 물었지만 서버 조회는 하루 한 번이다.
        assertThat(server.marketStatusCalls).hasValue(1);
    }

    @Test
    void businessDayPublishesEveryTick() throws Exception {
        fireEverySchedule();

        for (String jobType : new String[] {"collect", "collect-news", "collect-pre-open", "collect-intraday",
                "collect-post-close", "daily-briefing-morning", "daily-briefing-post-close"}) {
            verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq(jobType), anyString());
        }
    }

    @Test
    void failingStatusLookupPublishesAsOnAnOrdinaryBusinessDay() throws Exception {
        server.marketStatusStatus = 500;

        fireEverySchedule();

        for (String jobType : new String[] {"collect", "collect-news", "collect-pre-open", "collect-intraday",
                "collect-post-close", "daily-briefing-morning", "daily-briefing-post-close"}) {
            verify(kafka).send(eq(Topics.SCHEDULE_TICK), eq(jobType), anyString());
        }
        // 세 번 다시 시도한 뒤 쿨다운 동안은 더 묻지 않는다.
        assertThat(server.marketStatusCalls).hasValue(BrokerMarketDay.MAX_ATTEMPTS);
    }
}

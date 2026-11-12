package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import tools.jackson.databind.json.JsonMapper;

/** 장 운영 상태 조회로 휴장일을 판정한다. 규칙은 가짜 응답으로 검증한 것이며 실제 휴장일 응답은 아직 보지 못했다. */
class BrokerMarketDayTest {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    /** 시각을 테스트가 옮기는 시계. */
    private static final class MovableClock extends Clock {
        private Instant now;

        MovableClock(String instant) {
            this.now = Instant.parse(instant);
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return KST;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return Clock.fixed(now, zone);
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    private FakeBrokerServer server;
    private MovableClock clock;
    private final AtomicInteger limiterCalls = new AtomicInteger();
    private volatile boolean limiterOpen = true;
    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private final Logger marketDayLog = (Logger) LoggerFactory.getLogger(BrokerMarketDay.class);

    @BeforeEach
    void start() throws IOException {
        server = new FakeBrokerServer();
        // 2026-11-10(화) 10:00 한국 시간
        clock = new MovableClock("2026-11-10T01:00:00Z");
        logs.start();
        marketDayLog.addAppender(logs);
    }

    @AfterEach
    void stop() {
        marketDayLog.detachAppender(logs);
        server.close();
    }

    private BrokerMarketDay marketDay() {
        return marketDay(FakeBrokerServer.MARKET_STATUS);
    }

    private BrokerMarketDay marketDay(MarketStatusProperties status) {
        CallRateLimit limiter = maxWait -> {
            limiterCalls.incrementAndGet();
            return limiterOpen;
        };
        BrokerClient client = new BrokerClient(new BrokerProperties(server.baseUrl(), "ak", "as", 5, 10, BrokerProperties.DEFAULT_QUOTE_TIMEOUT_SECONDS),
                JsonMapper.builder().build(), clock, limiter);
        return new BrokerMarketDay(client, limiter, status, JsonMapper.builder().build(), clock, Duration.ZERO);
    }

    private List<String> warnMessages() {
        return logs.list.stream().filter(e -> e.getLevel().isGreaterOrEqual(Level.WARN))
                .map(ILoggingEvent::getFormattedMessage).toList();
    }

    @Test
    void sameCurrentAndStandardBusinessDateIsABusinessDay() {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261110");

        assertThat(marketDay().isBusinessDay()).isTrue();
    }

    @Test
    void standardBusinessDateLaggingTheCurrentDateIsAHoliday() {
        // 토요일에 직전 영업일(금요일)이 기준 영업일로 남아 있는 경우
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261107", "20261106");

        assertThat(marketDay().isBusinessDay()).isFalse();
    }

    @Test
    void dateFieldsNestedInsideTheBodyAreFound() {
        server.marketStatusBody = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER
                + ",\"dataBody\":{\"out\":{\"today_field\":\"20261009\",\"business_day_field\":\"20261008\"}}}";

        assertThat(marketDay().isBusinessDay()).isFalse();
    }

    @Test
    void successfulAnswerIsKeptForTheDayAndAskedAgainTheNextDay() {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261110");
        BrokerMarketDay marketDay = marketDay();

        for (int i = 0; i < 5; i++) {
            assertThat(marketDay.isBusinessDay()).isTrue();
        }
        assertThat(server.marketStatusCalls).hasValue(1);
        assertThat(server.tokenCalls).hasValue(1);

        // 다음 날(휴장일)에는 새로 묻는다.
        clock.advance(Duration.ofDays(1));
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261111", "20261110");
        assertThat(marketDay.isBusinessDay()).isFalse();
        assertThat(server.marketStatusCalls).hasValue(2);
    }

    // 휴장일 판정은 평일 08:00에 미리 하고 그날은 그 답을 쓴다. 이른 tick 과 장중 타이머가 같은 답을 받는다.
    @Test
    void theMorningDecisionSettlesTheHolidayForTheWholeDay() throws Exception {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261107", "20261106");
        BrokerMarketDay marketDay = marketDay();

        marketDay.decideToday();
        // 하루 동안 아무리 물어도 서버를 다시 부르지 않고, 휴장일이라는 같은 답이 나온다.
        for (int i = 0; i < 5; i++) {
            assertThat(marketDay.isBusinessDay()).isFalse();
        }

        assertThat(server.marketStatusCalls).hasValue(1);
    }

    @Test
    void theMorningDecisionRunsOnWeekdaysAtEightKoreanTimeFromSettings() throws Exception {
        Scheduled scheduled = BrokerMarketDay.class.getDeclaredMethod("decideToday").getAnnotation(Scheduled.class);

        assertThat(scheduled.cron()).isEqualTo("${collector.market-status.decide-cron}");
        assertThat(scheduled.zone()).isEqualTo("Asia/Seoul");
        // 기본값은 application.yml 이 원본이다. 08:00 은 가장 이른 tick(구성 종목 조회)과 같거나 앞이다.
        var yml = new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withInitializer(new org.springframework.boot.test.context.ConfigDataApplicationContextInitializer());
        yml.run(context -> assertThat(context.getEnvironment().getProperty("collector.market-status.decide-cron"))
                .isEqualTo("0 0 8 * * MON-FRI"));
    }

    @Test
    void lookupGoesThroughTheSharedCallLimit() {
        marketDay().isBusinessDay();

        assertThat(limiterCalls).hasValue(1);
    }

    @Test
    void lookupFailingTwiceThenSucceedingUsesTheAnswer() {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261107", "20261106");
        server.failMarketStatusCalls.set(2);

        assertThat(marketDay().isBusinessDay()).isFalse();

        assertThat(server.marketStatusCalls).hasValue(3);
        assertThat(warnMessages()).hasSize(2);
    }

    @Test
    void lookupFailingEveryAttemptTreatsTheDayAsBusinessDayAndWarns() {
        server.marketStatusStatus = 500;
        BrokerMarketDay marketDay = marketDay();

        assertThat(marketDay.isBusinessDay()).isTrue();

        assertThat(server.marketStatusCalls).hasValue(BrokerMarketDay.MAX_ATTEMPTS);
        assertThat(warnMessages()).hasSize(BrokerMarketDay.MAX_ATTEMPTS + 1);
        assertThat(warnMessages().getLast()).contains("treating today as a business day");
    }

    @Test
    void afterAFailedLookupItIsNotRetriedUntilTheCooldownPasses() {
        server.marketStatusStatus = 500;
        BrokerMarketDay marketDay = marketDay();
        marketDay.isBusinessDay();
        int callsAfterFailure = server.marketStatusCalls.get();

        // 1초 tick 이 매초 부르는 상황: 쿨다운 안에서는 서버를 다시 부르지 않는다.
        for (int i = 0; i < 10; i++) {
            clock.advance(Duration.ofSeconds(1));
            assertThat(marketDay.isBusinessDay()).isTrue();
        }
        assertThat(server.marketStatusCalls).hasValue(callsAfterFailure);

        // 쿨다운이 지나면 다시 묻고, 이번에는 휴장일 응답을 받는다.
        server.marketStatusStatus = 200;
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261109");
        clock.advance(BrokerMarketDay.FAILURE_COOLDOWN);
        assertThat(marketDay.isBusinessDay()).isFalse();
    }

    @Test
    void responseWithoutDateFieldsCountsAsAFailedLookup() {
        server.marketStatusBody = "{\"dataHeader\":" + FakeBrokerServer.OK_HEADER + ",\"dataBody\":{}}";

        assertThat(marketDay().isBusinessDay()).isTrue();

        assertThat(server.marketStatusCalls).hasValue(BrokerMarketDay.MAX_ATTEMPTS);
    }

    @Test
    void callLimitWithoutASlotFailsTheLookupWithoutCallingTheServer() {
        limiterOpen = false;

        assertThat(marketDay().isBusinessDay()).isTrue();

        assertThat(server.marketStatusCalls).hasValue(0);
        assertThat(server.tokenCalls).hasValue(0);
        assertThat(limiterCalls).hasValue(BrokerMarketDay.MAX_ATTEMPTS);
    }

    @Test
    void rejectedTokenDuringTheLookupIsReissuedAndTheAnswerIsUsed() {
        server.rejectAuthorizedCalls.set(1);
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261107", "20261106");

        assertThat(marketDay().isBusinessDay()).isFalse();

        assertThat(server.tokenCalls).hasValue(2);
        // 판정 조회 한 칸과 재시도 한 칸, 모두 같은 상한 장치를 거친다.
        assertThat(limiterCalls).hasValue(2);
    }

    @Test
    void slowServerEndsWithinTheLookupTimeLimitAsABusinessDay() {
        server.marketStatusDelayMs = 3_000;
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261109");
        var status = new MarketStatusProperties(FakeBrokerServer.MARKET_STATUS_PATH, "today_field",
                "business_day_field", Duration.ofMillis(400));
        BrokerMarketDay marketDay = marketDay(status);

        long start = System.nanoTime();
        boolean businessDay = marketDay.isBusinessDay();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        // 휴장일 응답이 늦게 오더라도 상한을 넘겨 기다리지 않고 영업일로 본다. 세 번을 다 기다리면 9초가 걸린다.
        assertThat(businessDay).isTrue();
        assertThat(elapsed).isLessThan(Duration.ofMillis(1_500));
        assertThat(warnMessages()).isNotEmpty();
        assertThat(warnMessages().getLast()).contains("treating today as a business day");
    }

    @Test
    void withoutAPathOrFieldNamesEveryDayIsABusinessDayWithOneStartupWarning() {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261109");
        BrokerMarketDay marketDay = marketDay(new MarketStatusProperties("", "today_field", "", null));

        assertThat(marketDay.isBusinessDay()).isTrue();
        assertThat(marketDay.isBusinessDay()).isTrue();

        assertThat(server.marketStatusCalls).hasValue(0);
        assertThat(server.tokenCalls).hasValue(0);
        assertThat(warnMessages()).hasSize(1);
    }

    @Test
    void theDayIsCountedByTheKoreanDateNotTheUtcDate() {
        // UTC 로는 전날 15:30 이지만 한국 시간으로는 11-10 00:30 이다. 한 시간 뒤에도 같은 한국 날짜라 조회는 한 번이다.
        clock = new MovableClock("2026-11-09T15:30:00Z");
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261110", "20261110");
        BrokerMarketDay marketDay = marketDay();

        marketDay.isBusinessDay();
        clock.advance(Duration.ofHours(1));
        marketDay.isBusinessDay();

        assertThat(server.marketStatusCalls).hasValue(1);
    }

    // 08:00 판정 로그는 실제로 판정한 경우와 조회 실패로 영업일로 본 경우를 구분해 남긴다.
    @Test
    void morningDecisionLogSaysDecidedWhenTheLookupSucceeded() {
        server.marketStatusBody = FakeBrokerServer.marketStatus("20261107", "20261106");

        marketDay().decideToday();

        assertThat(logs.list.stream().map(ILoggingEvent::getFormattedMessage))
                .contains("market day decided in advance businessDay=false")
                .noneMatch(m -> m.contains("not decided"));
    }

    @Test
    void morningDecisionLogSaysNotDecidedWhenTheLookupFailedAndTheDayIsTreatedAsABusinessDay() {
        server.marketStatusStatus = 500;
        BrokerMarketDay marketDay = marketDay();

        marketDay.decideToday();

        assertThat(logs.list.stream().map(ILoggingEvent::getFormattedMessage))
                .contains("market day not decided, treated as business day")
                .noneMatch(m -> m.startsWith("market day decided"));
        assertThat(marketDay.isBusinessDay()).isTrue();
    }
}

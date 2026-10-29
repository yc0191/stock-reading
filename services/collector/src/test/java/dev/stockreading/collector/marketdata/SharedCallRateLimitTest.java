package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.insight.InsightProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 장 마감 후 조회(해외 일봉과 기존 4종)가 시세 호출과 같은 상한 장치 하나를 나눠 쓰고, 겹쳐도 앱 전체 상한을 넘지 않는지 확인한다. */
class SharedCallRateLimitTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-22T07:30:00Z"), ZoneId.of("Asia/Seoul"));
    private static final List<String> SYMBOLS = List.of("TEST01", "TEST02", "TEST03", "TEST04", "TEST05");

    private FakeBrokerServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    // @Configuration 을 붙이면 같은 패키지를 스캔하는 다른 테스트가 이 설정까지 읽어 빈이 겹친다.
    // 명시적으로 등록할 때만 쓰이게 표시 없이 둔다.
    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, InsightProperties.class})
    @ComponentScan(basePackages = "dev.stockreading.collector.marketdata")
    static class Wiring {

        @Bean
        Clock clock() {
            return CLOCK;
        }

        @Bean
        ObjectMapper mapper() {
            return JsonMapper.builder().build();
        }
    }

    @Test
    void overseasCallsUseUpTheSameLimiterAsQuotes() throws IOException {
        server = new FakeBrokerServer();
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .withPropertyValues("COLLECTOR_MARKET_DATA_SOURCE=broker", "BROKER_BASE_URL=" + server.baseUrl(),
                        "BROKER_APP_KEY=k", "BROKER_APP_SECRET=s", "BROKER_MAX_CALLS_PER_SECOND=3")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(CallRateLimit.class);
                    BrokerInsightSource overseas = context.getBean(BrokerInsightSource.class);
                    BrokerMarketData quotes = context.getBean(BrokerMarketData.class);

                    for (int i = 0; i < 3; i++) {
                        overseas.overseasDailyPrice("NAS", "TESTA");
                    }
                    FetchResult result = quotes.fetch(List.of("TEST01"));

                    // 해외 일봉 3건이 1초 상한 3을 다 써서 바로 이어진 시세 호출은 자리를 얻지 못한다.
                    assertThat(server.overseasCalls.get()).isEqualTo(3);
                    assertThat(server.priceCalls.get()).isZero();
                    assertThat(result.failures()).singleElement()
                            .satisfies(f -> assertThat(f.type()).isEqualTo(FailureType.TICK_SKIPPED));
                });
    }

    @Test
    void overseasCallFailsWithoutCallingTheServerWhenNoSlotIsFree() throws IOException {
        server = new FakeBrokerServer();
        BrokerInsightSource overseas = source(wait -> false);

        assertThatThrownBy(() -> overseas.overseasDailyPrice("NYS", "TESTB"))
                .isInstanceOfSatisfying(BrokerApiException.class, e -> {
                    assertThat(e.failureType()).isEqualTo(FailureType.TICK_SKIPPED);
                    assertThat(e.exceptionType()).isEqualTo(BrokerMarketData.RATE_LIMITED);
                    assertThat(e.getMessage()).doesNotContain("TESTB");
                });
        assertThat(server.overseasCalls.get()).isZero();
        assertThat(server.tokenCalls.get()).isZero();
    }

    @Test
    void concurrentQuoteAndOverseasCallsStayUnderTheLimitInEveryOneSecondWindow() throws Exception {
        server = new FakeBrokerServer();
        int maxCalls = 5;
        // 상한 장치가 자리를 내준 바로 그 시각을 기록하려고, 장치가 읽은 마지막 시각을 스레드별로 남긴다.
        ThreadLocal<Long> lastRead = new ThreadLocal<>();
        LongSupplier clock = () -> {
            long now = System.nanoTime();
            lastRead.set(now);
            return now;
        };
        CallRateLimiter inner = new CallRateLimiter(maxCalls, Duration.ofSeconds(1), clock);
        ConcurrentLinkedQueue<Long> grants = new ConcurrentLinkedQueue<>();
        CallRateLimit shared = wait -> {
            boolean granted = inner.tryAcquire(wait);
            if (granted) {
                grants.add(lastRead.get());
            }
            return granted;
        };
        var broker = new BrokerProperties(server.baseUrl(), "k", "s", 5, maxCalls);
        var client = new BrokerClient(broker, JsonMapper.builder().build(), CLOCK);
        var quotes = new BrokerMarketData(client, new CollectorProperties(SYMBOLS, 1_000, 3_000), broker, shared);
        var overseas = new BrokerInsightSource(client, insightProperties(), shared);

        CompletableFuture<Void> quoteLoop = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 4; i++) {
                quotes.fetch(SYMBOLS);
            }
        });
        CompletableFuture<Void> overseasLoop = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < 12; i++) {
                try {
                    overseas.overseasDailyPrice("NAS", "TESTA");
                } catch (BrokerApiException e) {
                    // 자리를 얻지 못한 호출은 실패로 끝나고 서버에 가지 않는다.
                }
            }
        });
        CompletableFuture.allOf(quoteLoop, overseasLoop).get();

        List<Long> times = new ArrayList<>(grants);
        times.sort(null);
        // 서버에 간 호출 수가 상한 장치가 내준 자리 수와 같아야 우회 경로가 없다.
        assertThat(server.priceCalls.get() + server.overseasCalls.get()).isEqualTo(times.size());
        assertThat(server.overseasCalls.get()).isPositive();
        assertThat(server.priceCalls.get()).isPositive();
        long window = Duration.ofSeconds(1).toNanos();
        for (int i = 0; i < times.size(); i++) {
            int inWindow = 0;
            for (int j = i; j < times.size() && times.get(j) - times.get(i) < window; j++) {
                inWindow++;
            }
            assertThat(inWindow).as("1초 구간의 호출 수").isLessThanOrEqualTo(maxCalls);
        }
    }

    @Test
    void existingInsightCallsGoThroughTheLimiterWithoutBypass() throws IOException {
        server = new FakeBrokerServer();
        AtomicInteger grants = new AtomicInteger();
        BrokerInsightSource source = source(wait -> {
            grants.incrementAndGet();
            return true;
        });

        source.dailyPrice("TEST01");
        source.worldIndex("C");
        source.exchangeRateSummary();
        source.marketSummary();

        // 상한 장치가 내준 자리 수와 서버에 간 조회 수가 같아야 우회 경로가 없다. 토큰 발급은 세지 않는다.
        assertThat(server.insightCalls.get()).isEqualTo(4);
        assertThat(grants.get()).isEqualTo(4);
    }

    @Test
    void existingInsightCallsFailWithoutCallingTheServerWhenNoSlotIsFree() throws IOException {
        server = new FakeBrokerServer();
        BrokerInsightSource source = source(wait -> false);

        List<ThrowingCallable> calls = List.of(
                () -> source.dailyPrice("TEST01"),
                () -> source.worldIndex("C"),
                source::exchangeRateSummary,
                source::marketSummary);
        for (ThrowingCallable call : calls) {
            assertThatThrownBy(call).isInstanceOfSatisfying(BrokerApiException.class, e -> {
                assertThat(e.failureType()).isEqualTo(FailureType.TICK_SKIPPED);
                assertThat(e.exceptionType()).isEqualTo(BrokerMarketData.RATE_LIMITED);
            });
        }
        assertThat(server.insightCalls.get()).isZero();
        assertThat(server.tokenCalls.get()).isZero();
    }

    @Test
    void existingInsightCallsShareTheLimiterWithQuotes() throws IOException {
        server = new FakeBrokerServer();
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .withPropertyValues("COLLECTOR_MARKET_DATA_SOURCE=broker", "BROKER_BASE_URL=" + server.baseUrl(),
                        "BROKER_APP_KEY=k", "BROKER_APP_SECRET=s", "BROKER_MAX_CALLS_PER_SECOND=4")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    BrokerInsightSource source = context.getBean(BrokerInsightSource.class);
                    BrokerMarketData quotes = context.getBean(BrokerMarketData.class);

                    source.dailyPrice("TEST01");
                    source.worldIndex("C");
                    source.exchangeRateSummary();
                    source.marketSummary();
                    FetchResult result = quotes.fetch(List.of("TEST01"));

                    // 기존 4종이 1초 상한 4를 다 써서 바로 이어진 시세 호출은 자리를 얻지 못한다.
                    assertThat(server.insightCalls.get()).isEqualTo(4);
                    assertThat(server.priceCalls.get()).isZero();
                    assertThat(result.failures()).singleElement()
                            .satisfies(f -> assertThat(f.type()).isEqualTo(FailureType.TICK_SKIPPED));
                });
    }

    @Test
    void overseasCallReturnsTheServerBodyWhenASlotIsFree() throws IOException {
        server = new FakeBrokerServer();
        BrokerInsightSource overseas = source(wait -> true);

        assertThat(overseas.overseasDailyPrice("AMX", "TESTC")).contains("\"out2\":[]");
        assertThat(server.overseasCalls.get()).isEqualTo(1);
    }

    private BrokerInsightSource source(CallRateLimit limiter) {
        var broker = new BrokerProperties(server.baseUrl(), "k", "s", 5, 10);
        var client = new BrokerClient(broker, JsonMapper.builder().build(), CLOCK);
        return new BrokerInsightSource(client, insightProperties(), limiter);
    }

    private static InsightProperties insightProperties() {
        return new InsightProperties(new InsightProperties.Schedule(false, "0 30 16 * * MON-FRI"), "0", 30,
                List.of(), 30, 100);
    }
}

package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.holdings.QuoteSymbolsPort;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.insight.InsightProperties;
import dev.stockreading.collector.insight.InsightPublisher;
import dev.stockreading.collector.insight.IntradayProperties;
import dev.stockreading.collector.marketdata.BrokerProperties;
import dev.stockreading.collector.marketdata.MarketStatusProperties;
import dev.stockreading.collector.marketdata.FakeBrokerServer;
import dev.stockreading.collector.marketdata.PriceCheckProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 로 조립한 수집기(초당 호출 상한 기본값 10, 종목 9개)에서 타이머 실행부터 market.snapshot 과
 * 건너뜀 이벤트 발행까지를 가짜 시세 서버로 본다. Kafka 는 mock 이고 실제 서버는 호출하지 않는다.
 */
class CollectTimerFlowTest {

    // 수요일 10:00(한국 시간): 시세 시간대 안
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-21T01:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final List<String> SYMBOLS = IntStream.rangeClosed(1, 9).mapToObj(i -> "TEST0" + i).toList();

    // @Configuration 을 달지 않는다. 달면 같은 패키지를 스캔하는 다른 조립 테스트가 이 설정까지 읽어 빈이 겹친다.
    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, MarketStatusProperties.class, InsightProperties.class,
            PriceCheckProperties.class, QuoteHoursProperties.class, IntradayProperties.class})
    @ComponentScan(basePackages = "dev.stockreading.collector.marketdata")
    // 수집기 타이머 쪽은 현재가 흐름과 마감 후 타이머만 올린다. 뉴스·공시 발행기는 아래 mock 이다.
    @ComponentScan(basePackages = "dev.stockreading.collector.schedule", useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = ".*\\.(CollectorJobs|QuotePublisher|CollectTimer|PostCloseTimer)"))
    static class Wiring {

        @Bean
        QuoteSymbolsPort quoteSymbols() {
            return () -> SYMBOLS;
        }

        @Bean
        Clock clock() {
            return CLOCK;
        }

        @Bean
        ObjectMapper mapper() {
            return JsonMapper.builder().build();
        }

        @SuppressWarnings("unchecked")
        @Bean
        KafkaTemplate<String, String> kafka() {
            return mock(KafkaTemplate.class);
        }

        @Bean
        NewsPublisher newsPublisher() {
            return mock(NewsPublisher.class);
        }

        @Bean
        FilingPublisher filingPublisher() {
            return mock(FilingPublisher.class);
        }
    }

    @EnableScheduling
    static class Scheduling {
    }

    private FakeBrokerServer server;
    private final CountDownLatch release = new CountDownLatch(1);

    @AfterEach
    void stop() {
        release.countDown();
        if (server != null) {
            server.close();
        }
    }

    private ApplicationContextRunner runner() throws IOException {
        server = new FakeBrokerServer();
        return new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .withPropertyValues(
                        "COLLECTOR_MARKET_DATA_SOURCE=broker",
                        "BROKER_BASE_URL=" + server.baseUrl(),
                        "BROKER_APP_KEY=k",
                        "BROKER_APP_SECRET=s",
                        "BROKER_PRICE_CHECK_ENABLED=false",
                        "COLLECTOR_SCHEDULE_ENABLED=true");
    }

    @SuppressWarnings("unchecked")
    @Test
    void roundsThatFireWhileAFetchIsSlowAreSkippedPerSymbolAndNotFetchedLater() throws IOException {
        runner().run(context -> {
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            CollectTimer timer = context.getBean(CollectTimer.class);

            // 응답이 1초 넘게 걸리는 동안 다음 두 회차의 타이머가 온다.
            server.priceDelayMs = 1_500;
            timer.collect();
            timer.collect();
            timer.collect();

            // 종목 9개에 회차당 8개라 첫 회차는 8종목을 부르고, 건너뛴 회차는 다음 차례 8종목만 기록한다.
            verify(kafka, timeout(10_000).times(8)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
            ArgumentCaptor<String> failures = ArgumentCaptor.forClass(String.class);
            verify(kafka, times(16)).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), failures.capture());
            assertThat(failures.getAllValues()).allSatisfy(json -> {
                assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", json)).isEmpty();
                assertThat(json).contains("\"failureType\":\"TICK_SKIPPED\"")
                        .contains("\"exceptionType\":\"SupersededByNewerTick\"")
                        .doesNotContain("RateLimited");
            });
            // 건너뛴 회차를 앞 회차가 끝난 뒤 몰아서 조회하지 않는다. 몰아서 조회하면 같은 1초에 호출이 몰려 상한에 걸린다.
            assertThat(server.priceCalls.get()).isEqualTo(8);
        });
    }

    @Test
    void aLongPostCloseRunDoesNotStopTheOneSecondQuotesOrTheNews() throws IOException {
        InsightPublisher insight = mock(InsightPublisher.class);
        CountDownLatch postCloseEntered = new CountDownLatch(1);
        doAnswer(call -> {
            postCloseEntered.countDown();
            release.await(10, TimeUnit.SECONDS);
            return null;
        }).when(insight).publishAll(anyString(), anyString());

        runner().withUserConfiguration(Scheduling.class)
                .withConfiguration(AutoConfigurations.of(TaskSchedulingAutoConfiguration.class))
                .withBean(InsightPublisher.class, () -> insight)
                .withPropertyValues(
                        // withBean 은 application.yml 을 읽기 전에 InsightPublisher 의 출처 조건을 보므로 키를 직접 준다.
                        "collector.market-data.source=broker",
                        // 타이머 스레드 수는 application.yml 값을 그대로 쓴다.
                        "COLLECTOR_TICK_INTERVAL_MS=200",
                        "collector.schedule.news-interval-ms=200",
                        "COLLECTOR_INSIGHT_SCHEDULE_ENABLED=true",
                        "COLLECTOR_INSIGHT_CRON=* * * * * *")
                .run(context -> {
                    NewsPublisher news = context.getBean(NewsPublisher.class);
                    assertThat(postCloseEntered.await(5, TimeUnit.SECONDS)).isTrue();
                    int quotesWhenBlocked = server.priceCalls.get();
                    int newsWhenBlocked = mockingDetails(news).getInvocations().size();

                    // 마감 후 작업이 타이머 스레드 하나를 잡고 있는 동안에도 현재가와 뉴스가 계속 돈다.
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while ((server.priceCalls.get() < quotesWhenBlocked + 18
                            || mockingDetails(news).getInvocations().size() < newsWhenBlocked + 2)
                            && System.nanoTime() < deadline) {
                        Thread.sleep(50);
                    }
                    assertThat(server.priceCalls.get()).isGreaterThanOrEqualTo(quotesWhenBlocked + 18);
                    assertThat(mockingDetails(news).getInvocations().size()).isGreaterThanOrEqualTo(newsWhenBlocked + 2);
                    // 매초 오는 마감 후 회차는 앞 회차가 끝나지 않았으므로 다시 돌지 않는다.
                    verify(insight, times(1)).publishAll(anyString(), anyString());
                    release.countDown();
                });
    }
}

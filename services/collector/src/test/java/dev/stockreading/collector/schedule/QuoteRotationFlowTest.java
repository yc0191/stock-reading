package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.members.MembersPublisher;
import dev.stockreading.collector.holdings.QuoteSymbolsPort;
import dev.stockreading.collector.marketdata.BrokerProperties;
import dev.stockreading.collector.marketdata.FakeBrokerServer;
import dev.stockreading.collector.marketdata.MarketStatusProperties;
import dev.stockreading.collector.marketdata.PriceCheckProperties;
import dev.stockreading.collector.insight.InsightProperties;
import dev.stockreading.collector.insight.IntradayProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 가짜 시세 서버로 종목이 회차당 수보다 많을 때의 차례 돌기를 여러 회차에 걸쳐 본다:
 * 모든 종목이 정해진 회차 안에 한 번 이상 market.snapshot 으로 나가고, 회차당 호출이 회차당 수를 넘지 않는다.
 */
class QuoteRotationFlowTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-21T01:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final int PER_TICK = 8;
    private static volatile List<String> symbols = List.of();

    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, MarketStatusProperties.class,
            InsightProperties.class, PriceCheckProperties.class, QuoteHoursProperties.class, IntradayProperties.class})
    @ComponentScan(basePackages = "dev.stockreading.collector.marketdata")
    @ComponentScan(basePackages = "dev.stockreading.collector.schedule", useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = ".*\\.(CollectorJobs|QuotePublisher)"))
    static class Wiring {

        @Bean
        QuoteSymbolsPort quoteSymbols() {
            return () -> symbols;
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

        @Bean
        MembersPublisher membersPublisher() {
            return mock(MembersPublisher.class);
        }
    }

    private FakeBrokerServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.close();
        }
    }

    private static List<String> symbols(int count) {
        return IntStream.rangeClosed(1, count).mapToObj(i -> String.format("TEST%02d", i)).toList();
    }

    @SuppressWarnings("unchecked")
    private void rotate(int symbolCount) throws IOException {
        symbols = symbols(symbolCount);
        server = new FakeBrokerServer();
        new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(Wiring.class)
                .withPropertyValues(
                        "COLLECTOR_MARKET_DATA_SOURCE=broker",
                        "BROKER_BASE_URL=" + server.baseUrl(),
                        "BROKER_APP_KEY=k",
                        "BROKER_APP_SECRET=s",
                        "BROKER_PRICE_CHECK_ENABLED=false",
                        // 회차를 이어서 돌리므로 초당 상한에 걸리지 않게 크게 둔다. 이 테스트는 상한이 아니라 차례를 본다.
                        "BROKER_MAX_CALLS_PER_SECOND=1000")
                .run(context -> {
                    KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
                    CollectorJobs jobs = context.getBean(CollectorJobs.class);
                    int rounds = (symbolCount + PER_TICK - 1) / PER_TICK;

                    for (int round = 0; round < rounds; round++) {
                        int before = server.priceCalls.get();
                        jobs.run(CollectorJobs.Job.COLLECT);
                        assertThat(server.priceCalls.get() - before).isLessThanOrEqualTo(PER_TICK);
                    }

                    ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
                    verify(kafka, org.mockito.Mockito.atLeast(1)).send(eq(Topics.MARKET_SNAPSHOT), key.capture(), anyString());
                    Set<String> published = new HashSet<>(key.getAllValues());
                    assertThat(published).containsExactlyInAnyOrderElementsOf(symbols);
                    verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), anyString());
                });
    }

    @Test
    void elevenSymbolsAreAllPublishedWithinTwoRounds() throws IOException {
        rotate(11);
    }

    @Test
    void twentySymbolsAreAllPublishedWithinThreeRounds() throws IOException {
        rotate(20);
    }
}

package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.edgar.ExchangePort;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.holdings.HoldingsPort;
import dev.stockreading.collector.holdings.QuoteSymbolsPort;
import dev.stockreading.collector.insight.InsightProperties;
import dev.stockreading.collector.insight.IntradayProperties;
import dev.stockreading.collector.marketdata.BrokerProperties;
import dev.stockreading.collector.marketdata.FakeBrokerServer;
import dev.stockreading.collector.marketdata.MarketStatusProperties;
import dev.stockreading.collector.marketdata.PriceCheckProperties;
import dev.stockreading.collector.members.MembersPublisher;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 가짜 리허설(#64): application.yml 로 조립한 수집기의 마감 후 수집이 가짜 시세 서버의 연결 끊김을 받고, 실패한 조회만
 * 재시도 간격마다 다시 부른 뒤 마감 후 판 시작 알림을 한 번 내는 것을 본다. 간격만 300ms 로 줄이고 횟수(3)와 규칙은
 * 기본값이다. 실제 시세 서버와 Kafka 는 부르지 않는다(Kafka 는 mock).
 */
class PostCloseRetryRehearsalTest {

    private static final long INTERVAL_MS = 300;
    // 종목 2개 + 세계지수 대륙 3개 + 환율종합 + 시장종합.
    private static final int CALLS_PER_ROUND = 7;

    // @Configuration 을 달지 않는다. 달면 같은 패키지를 스캔하는 다른 조립 테스트가 이 설정까지 읽어 빈이 겹친다.
    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, MarketStatusProperties.class,
            InsightProperties.class, PriceCheckProperties.class, QuoteHoursProperties.class, IntradayProperties.class})
    @ComponentScan(basePackages = {"dev.stockreading.collector.marketdata", "dev.stockreading.collector.insight"})
    @ComponentScan(basePackages = "dev.stockreading.collector.schedule", useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = ".*\\.(CollectorJobs|QuotePublisher|DailyCollectionPublisher)"))
    static class Wiring {

        @Bean
        QuoteSymbolsPort quoteSymbols() {
            return () -> List.of("TEST01", "TEST02");
        }

        @Bean
        HoldingsPort holdings() {
            return mock(HoldingsPort.class);
        }

        @Bean
        ExchangePort exchanges() {
            return mock(ExchangePort.class);
        }

        // 재시도 대기가 실제로 흐르도록 실제 시계를 쓴다.
        @Bean
        Clock clock() {
            return Clock.system(ZoneId.of("Asia/Seoul"));
        }

        @Bean
        ObjectMapper mapper() {
            return JsonMapper.builder().build();
        }

        @SuppressWarnings("unchecked")
        @Bean
        KafkaTemplate<String, String> kafka() {
            KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
            when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
            return kafka;
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
                        "COLLECTOR_DAILY_BRIEFING_ENABLED=true",
                        "COLLECTOR_INSIGHT_RETRY_INTERVAL=" + INTERVAL_MS + "ms");
    }

    @SuppressWarnings("unchecked")
    @Test
    void aRoundThatLosesEveryConnectionIsRecoveredByTheFirstRetryAndTheNoticeGoesOutOnceWithNothingMissing()
            throws IOException {
        runner().run(context -> {
            server.dropInsightCalls.set(CALLS_PER_ROUND);
            long started = System.nanoTime();

            assertThat(context.getBean(CollectorJobs.class).run(CollectorJobs.Job.COLLECT_POST_CLOSE))
                    .isEqualTo(CollectorJobs.Outcome.STARTED);
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;

            // 첫 회차 7건이 모두 끊기고, 첫 재시도에서 그 7건만 다시 불러 모두 받는다. 두 번째 재시도는 없다.
            assertThat(server.insightCallLog).hasSize(2 * CALLS_PER_ROUND);
            assertThat(server.insightCallLog.subList(0, CALLS_PER_ROUND)).allMatch(c -> c.endsWith(" dropped"));
            assertThat(server.insightCallLog.subList(CALLS_PER_ROUND, 2 * CALLS_PER_ROUND))
                    .allMatch(c -> c.endsWith(" answered"));
            assertThat(elapsedMs).isGreaterThanOrEqualTo(INTERVAL_MS).isLessThan(2 * INTERVAL_MS + 2_000);

            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            verify(kafka, times(CALLS_PER_ROUND)).send(eq(Topics.MARKET_INSIGHT), anyString(), anyString());
            JsonNode notice = onlyNotice(kafka);
            assertThat(notice.path("payload").path("edition").asString()).isEqualTo("post-close");
            assertThat(notice.path("payload").path("missingSources")).isEmpty();
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void aServerThatStaysDownIsCalledFourTimesIntervalApartAndTheNoticeListsEveryKindOnce() throws IOException {
        runner().run(context -> {
            server.dropInsightCalls.set(-1);
            long started = System.nanoTime();

            context.getBean(CollectorJobs.class).run(CollectorJobs.Job.COLLECT_POST_CLOSE);
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;

            // 첫 회차와 재시도 3번, 모두 끊김. 재시도는 회차 시작에서 간격의 1·2·3배 시각이다.
            assertThat(server.insightCallLog).hasSize(4 * CALLS_PER_ROUND).allMatch(c -> c.endsWith(" dropped"));
            assertThat(elapsedMs).isGreaterThanOrEqualTo(3 * INTERVAL_MS).isLessThan(3 * INTERVAL_MS + 3_000);

            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            verify(kafka, times(0)).send(eq(Topics.MARKET_INSIGHT), anyString(), anyString());
            JsonNode notice = onlyNotice(kafka);
            assertThat(notice.path("payload").path("missingSources").valueStream().map(JsonNode::asString).toList())
                    .containsExactly("daily-price", "world-index", "exchange-rate-summary", "market-summary");
        });
    }

    private static JsonNode onlyNotice(KafkaTemplate<String, String> kafka) throws IOException {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(1)).send(eq(Topics.DAILY_COLLECTION_COMPLETED), anyString(), value.capture());
        assertThat(ContractSchemas.validate("daily.collection.completed.v1.schema.json", value.getValue())).isEmpty();
        return JsonMapper.builder().build().readTree(value.getValue());
    }
}

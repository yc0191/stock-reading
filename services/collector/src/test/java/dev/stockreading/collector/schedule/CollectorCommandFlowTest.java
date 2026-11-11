package dev.stockreading.collector.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.IntStream;

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
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.insight.InsightProperties;
import dev.stockreading.collector.insight.IntradayProperties;
import dev.stockreading.collector.marketdata.BrokerProperties;
import dev.stockreading.collector.marketdata.MarketStatusProperties;
import dev.stockreading.collector.marketdata.FakeBrokerServer;
import dev.stockreading.collector.marketdata.PriceCheckProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 로 조립한 수집기(타이머 꺼짐 기본값, 종목 3개)에서 수동 실행 명령을 받아 가짜 시세 서버 조회와
 * market.snapshot 발행까지를 본다. Kafka 는 mock 이고 실제 서버는 호출하지 않는다.
 */
class CollectorCommandFlowTest {

    // 일요일 22:00(한국 시간): 시세 시간대 밖이고 평일도 아니다.
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-25T13:00:00Z"), ZoneId.of("Asia/Seoul"));
    private static final List<String> SYMBOLS = IntStream.rangeClosed(1, 3).mapToObj(i -> "TEST0" + i).toList();
    private static final String SAMPLE_EVENT_ID = "6f1d2c3b-4a59-4e7f-8d10-2b3c4d5e6f70";

    // @Configuration 을 달지 않는다. 달면 같은 패키지를 스캔하는 다른 조립 테스트가 이 설정까지 읽어 빈이 겹친다.
    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, MarketStatusProperties.class, InsightProperties.class,
            PriceCheckProperties.class, QuoteHoursProperties.class, IntradayProperties.class})
    @ComponentScan(basePackages = "dev.stockreading.collector.marketdata")
    // 실행 입구·현재가 흐름·명령 리스너와 타이머를 올린다. 타이머는 켜기 설정이 꺼져 있어 빈이 만들어지지 않아야 한다.
    @ComponentScan(basePackages = "dev.stockreading.collector.schedule", useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = ".*\\.(CollectorJobs|QuotePublisher|CollectorCommandListener|CollectTimer)"))
    static class Wiring {

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
                        "BROKER_PRICE_CHECK_ENABLED=false",
                        "collector.symbols=" + String.join(",", SYMBOLS));
    }

    @SuppressWarnings("unchecked")
    @Test
    void aCollectCommandFetchesQuotesNowEvenOutsideQuoteHoursWithTheTimerOff() throws IOException {
        runner().run(context -> {
            assertThat(context).doesNotHaveBean(CollectTimer.class);
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            CollectorCommandListener listener = context.getBean(CollectorCommandListener.class);

            listener.onCommand(ContractSchemas.sample("collector.command.sample.json"));

            ArgumentCaptor<String> snapshots = ArgumentCaptor.forClass(String.class);
            verify(kafka, timeout(10_000).times(3)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), snapshots.capture());
            assertThat(snapshots.getAllValues()).allSatisfy(json ->
                    assertThat(ContractSchemas.validate("market.snapshot.v1.schema.json", json)).isEmpty());
            assertThat(server.priceCalls.get()).isEqualTo(3);
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void aSecondCommandWhileTheFirstRoundRunsIsSkippedWithoutSkipEventsAndARepeatedEventIdIsNotRunAgain()
            throws IOException {
        runner().run(context -> {
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            CollectorCommandListener listener = context.getBean(CollectorCommandListener.class);
            String sample = ContractSchemas.sample("collector.command.sample.json");

            server.priceDelayMs = 1_500;
            listener.onCommand(sample);
            // 다른 eventId 로 같은 작업을 다시 보낸다. 앞 회차가 돌고 있으므로 건너뛴다.
            listener.onCommand(sample.replace(SAMPLE_EVENT_ID, "7a2e3d4c-5b60-4f80-9e21-3c4d5e6f7081"));

            verify(kafka, timeout(10_000).times(3)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
            // 사람이 더 보낸 명령은 밀린 타이머 회차가 아니므로 건너뜀 이벤트로 남기지 않는다.
            verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), anyString());

            // 앞 회차가 끝난 뒤 같은 eventId 가 다시 와도(재전송) 다시 조회하지 않는다.
            server.priceDelayMs = 0;
            listener.onCommand(sample);
            Thread.sleep(500);
            assertThat(server.priceCalls.get()).isEqualTo(3);
        });
    }
}

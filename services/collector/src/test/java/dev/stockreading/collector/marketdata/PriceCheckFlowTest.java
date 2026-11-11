package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;

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
import org.springframework.test.util.ReflectionTestUtils;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.insight.InsightProperties;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 과 환경변수 키로 조립한 수집기에 가짜 서버 응답을 넣어, 칸이 맞지 않는 종목은 실패 이벤트로,
 * 나머지 종목은 시세 이벤트로 발행되는지 본다. Kafka 는 mock 이고 실제 서버는 호출하지 않는다.
 */
class PriceCheckFlowTest {

    private static final String NOW = "2026-10-21T10:00:00+09:00";

    // @Configuration 을 달지 않는다. 달면 같은 패키지를 스캔하는 다른 조립 테스트가 이 설정까지 읽어 빈이 겹친다.
    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, InsightProperties.class,
            PriceCheckProperties.class})
    @ComponentScan(basePackages = "dev.stockreading.collector.marketdata")
    // 다른 패키지의 현재가 발행기는 이 하나만 올린다.
    @ComponentScan(basePackages = "dev.stockreading.collector.schedule", useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = ".*\\.QuotePublisher"))
    static class Wiring {

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
            return mock(KafkaTemplate.class);
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
                        "BROKER_APP_KEY=key-secret-value",
                        "BROKER_APP_SECRET=app-secret-value",
                        "collector.symbols=TEST01,TEST02,TEST03",
                        "BROKER_PRICE_CHECK_BASE_FIELDS=" + PriceChecks.BASE + "," + PriceChecks.BASE_ALT,
                        "BROKER_PRICE_CHECK_CHANGE_FIELD=" + PriceChecks.CHANGE,
                        "BROKER_PRICE_CHECK_CHANGE_SIGN_FIELD=" + PriceChecks.SIGN,
                        "BROKER_PRICE_CHECK_SIGN_UP=U",
                        "BROKER_PRICE_CHECK_SIGN_FLAT=F",
                        "BROKER_PRICE_CHECK_SIGN_DOWN=D",
                        "BROKER_PRICE_CHECK_RATE_FIELD=" + PriceChecks.RATE,
                        "BROKER_PRICE_CHECK_HIGH_FIELD=" + PriceChecks.HIGH,
                        "BROKER_PRICE_CHECK_LOW_FIELD=" + PriceChecks.LOW);
    }

    private static void collect(Object publisher) {
        ReflectionTestUtils.invokeMethod(publisher, "publishCollected", "collect-" + NOW, NOW);
    }

    @SuppressWarnings("unchecked")
    @Test
    void mismatchedSymbolIsPublishedAsFieldMismatchFailureAndTheOthersAsSnapshots() throws IOException {
        runner().run(context -> {
            String broken = PriceChecks.body("12345", "10000", "100", "D", "-1.00", "10100", "9800");
            server.priceBody = PriceChecks.body("9900", "10000", "100", "D", "-1.00", "10100", "9800");
            server.priceBodyBySymbol.put("TEST02", broken);
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            Object publisher = context.getBean("quotePublisher");

            collect(publisher);

            ArgumentCaptor<String> snapshotKeys = ArgumentCaptor.forClass(String.class);
            verify(kafka, times(2)).send(eq(Topics.MARKET_SNAPSHOT), snapshotKeys.capture(), anyString());
            assertThat(snapshotKeys.getAllValues()).containsExactly("TEST01", "TEST03");

            ArgumentCaptor<String> failure = ArgumentCaptor.forClass(String.class);
            verify(kafka).send(eq(Topics.MARKET_SNAPSHOT_FAILED), eq("TEST02"), failure.capture());
            assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", failure.getValue())).isEmpty();
            JsonNode payload = JsonMapper.builder().build().readTree(failure.getValue()).path("payload");
            assertThat(payload.path("failureType").asString()).isEqualTo("FIELD_MISMATCH");
            assertThat(payload.path("exceptionType").asString()).isEqualTo("FieldMismatch:CHANGE+RATE+HIGH_LOW");
            // 원문은 trim 외에 가공하지 않는다.
            assertThat(payload.path("responseRaw").asString()).isEqualTo(broken);
            assertThat(payload.path("tickSucceeded").asInt()).isEqualTo(2);
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void turningTheCheckOffPublishesTheMismatchedQuoteAsASnapshot() throws IOException {
        runner().withPropertyValues("BROKER_PRICE_CHECK_ENABLED=false").run(context -> {
            server.priceBody = PriceChecks.body("12345", "10000", "100", "D", "-1.00", "10100", "9800");
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);

            collect(context.getBean("quotePublisher"));

            verify(kafka, times(3)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
            verify(kafka, times(0)).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), anyString());
        });
    }
}

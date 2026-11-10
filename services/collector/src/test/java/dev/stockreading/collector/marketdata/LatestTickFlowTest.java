package dev.stockreading.collector.marketdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.insight.InsightProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 로 조립한 수집기(초당 호출 상한 기본값 10)에 종목 9개를 두고, 쌓인 tick 묶음이 와도
 * 가장 최근 tick 하나만 조회해 상한을 넘지 않는지 가짜 서버로 본다. Kafka 는 mock 이고 실제 서버는 호출하지 않는다.
 */
class LatestTickFlowTest {

    private static final ZoneId SEOUL = ZoneId.of("Asia/Seoul");
    private static final List<String> SYMBOLS = IntStream.rangeClosed(1, 9).mapToObj(i -> "TEST0" + i).toList();

    // @Configuration 을 달지 않는다. 달면 같은 패키지를 스캔하는 다른 조립 테스트가 이 설정까지 읽어 빈이 겹친다.
    @EnableConfigurationProperties({CollectorProperties.class, BrokerProperties.class, MarketStatusProperties.class, InsightProperties.class,
            PriceCheckProperties.class})
    @ComponentScan(basePackages = "dev.stockreading.collector.marketdata")
    // 다른 패키지의 tick 처리기는 이 하나만 올린다.
    @ComponentScan(basePackages = "dev.stockreading.collector.schedule", useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = ".*\\.TickListener"))
    static class Wiring {

        @Bean
        Clock clock() {
            return Clock.system(SEOUL);
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
                        "BROKER_APP_KEY=k",
                        "BROKER_APP_SECRET=s",
                        "BROKER_PRICE_CHECK_ENABLED=false",
                        // 테스트가 느린 응답을 흉내 내는 동안 tick 이 오래된 tick 으로 건너뛰어지지 않게 넉넉히 둔다.
                        "COLLECTOR_MAX_TICK_AGE_MS=30000",
                        "collector.symbols=" + String.join(",", SYMBOLS));
    }

    /** 지금부터 secondsAgo 초 전 시각의 tick 들. 같은 초가 겹치지 않게 초 단위로 자른다. */
    private static List<String> ticks(int... secondsAgo) throws IOException {
        OffsetDateTime now = OffsetDateTime.now(SEOUL).truncatedTo(ChronoUnit.SECONDS);
        String sample = ContractSchemas.sample("schedule.tick.sample.json");
        return IntStream.of(secondsAgo).mapToObj(ago -> {
            String at = now.minusSeconds(ago).format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"));
            return sample.replace("\"scheduledFor\": \"2026-10-20T16:00:00+09:00\"", "\"scheduledFor\": \"" + at + "\"")
                    .replace("collect-2026-10-20T16:00:00+09:00", "collect-" + at);
        }).toList();
    }

    private static List<String> failurePayloads(KafkaTemplate<String, String> kafka, int times) {
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(times)).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), value.capture());
        return value.getAllValues();
    }

    @SuppressWarnings("unchecked")
    @Test
    void threePiledUpTicksCallOnlyOnceAndStayUnderTheLimit() throws IOException {
        List<String> piled = ticks(2, 1, 0);
        runner().run(context -> {
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);

            ReflectionTestUtils.invokeMethod(context.getBean("tickListener"), "onTicks", piled);

            assertThat(server.priceCalls.get()).isEqualTo(9);
            verify(kafka, times(9)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
            List<String> failures = failurePayloads(kafka, 18);
            assertThat(failures).allSatisfy(json -> {
                assertThat(ContractSchemas.validate("market.snapshot.failed.v1.schema.json", json)).isEmpty();
                assertThat(json).contains("\"exceptionType\":\"SupersededByNewerTick\"")
                        .doesNotContain("RateLimited");
            });
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void ticksThatPileUpBehindASlowResponseAreReleasedWithOneFetch() throws IOException {
        List<String> first = ticks(0);
        runner().run(context -> {
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            Object listener = context.getBean("tickListener");

            // 응답이 1초 넘게 걸리는 동안 다음 tick 들이 쌓인다.
            server.priceDelayMs = 1_100;
            ReflectionTestUtils.invokeMethod(listener, "onTicks", first);
            verify(kafka, times(9)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());

            server.priceDelayMs = 0;
            clearInvocations(kafka);
            // 첫 tick 과 같은 초가 되어 중복으로 버려지지 않게 지금 이후 시각으로 만든다. 최신이 앞에 와도 고른다.
            List<String> piled = ticks(-1, -2, -3).reversed();
            ReflectionTestUtils.invokeMethod(listener, "onTicks", piled);

            assertThat(server.priceCalls.get()).isEqualTo(18);
            verify(kafka, times(9)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
            assertThat(failurePayloads(kafka, 18))
                    .allSatisfy(json -> assertThat(json).contains("\"exceptionType\":\"SupersededByNewerTick\""));
        });
    }

    /** 새 규칙이 없을 때의 모습: 쌓인 tick 을 하나씩 모두 조회하면 같은 1초에 호출이 몰려 상한에 걸린다. */
    @SuppressWarnings("unchecked")
    @Test
    void fetchingEveryPiledUpTickOneByOneWouldHitTheLimit() throws IOException {
        List<String> piled = ticks(2, 1, 0);
        runner().run(context -> {
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            Object listener = context.getBean("tickListener");

            for (String tick : piled) {
                ReflectionTestUtils.invokeMethod(listener, "onTick", tick);
            }

            ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
            verify(kafka, atLeast(1)).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), value.capture());
            assertThat(value.getAllValues()).anyMatch(json -> json.contains("\"exceptionType\":\"RateLimited\""));
            assertThat(value.getAllValues()).noneMatch(json -> json.contains("SupersededByNewerTick"));
        });
    }

    /** 리스너 컨테이너는 만들되 시작하지 않는다. 브로커에 붙지 않고 Spring Kafka 의 리스너 어댑터만 쓴다. */
    @EnableKafka
    static class ListenerContainers {

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
            var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
            factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)));
            factory.setAutoStartup(false);
            return factory;
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    void theRegisteredKafkaListenerHandsAPolledBatchToTheListenerAsOneCall() throws IOException {
        List<String> piled = ticks(2, 1, 0);
        runner().withUserConfiguration(ListenerContainers.class).run(context -> {
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);
            MessageListenerContainer container = context.getBean(KafkaListenerEndpointRegistry.class)
                    .getListenerContainers().stream()
                    .filter(c -> "collector".equals(c.getGroupId()))
                    .findFirst().orElseThrow();
            Object messageListener = container.getContainerProperties().getMessageListener();
            assertThat(messageListener).isInstanceOf(BatchMessageListener.class);

            List<ConsumerRecord<String, String>> records = IntStream.range(0, piled.size())
                    .mapToObj(i -> new ConsumerRecord<>(Topics.SCHEDULE_TICK, 0, i, "collect", piled.get(i)))
                    .toList();
            // 컨테이너가 부르는 형태 그대로 부른다(확인 응답과 consumer 는 이 리스너가 쓰지 않는다).
            ((BatchMessageListener<String, String>) messageListener).onMessage(records, null, null);

            assertThat(server.priceCalls.get()).isEqualTo(9);
            verify(kafka, times(9)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
            assertThat(failurePayloads(kafka, 18))
                    .allSatisfy(json -> assertThat(json).contains("\"exceptionType\":\"SupersededByNewerTick\""));
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    void aSingleTickIsFetchedAsBefore() throws IOException {
        List<String> one = ticks(0);
        runner().run(context -> {
            KafkaTemplate<String, String> kafka = context.getBean(KafkaTemplate.class);

            ReflectionTestUtils.invokeMethod(context.getBean("tickListener"), "onTicks", one);

            assertThat(server.priceCalls.get()).isEqualTo(9);
            verify(kafka, times(9)).send(eq(Topics.MARKET_SNAPSHOT), anyString(), anyString());
            verify(kafka, never()).send(eq(Topics.MARKET_SNAPSHOT_FAILED), anyString(), anyString());
        });
    }
}

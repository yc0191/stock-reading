package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.marketdata.BrokerApiException;
import dev.stockreading.collector.marketdata.FailureType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class InsightTickListenerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:30:01Z"), ZoneId.of("Asia/Seoul"));
    private static final String SCHEMA = "market.insight.v1.schema.json";
    // 안쪽 공백과 숫자 표기가 그대로 남는지 보려고 일부러 어색하게 만든 가짜 응답이다.
    private static final String RAW = "{\"dataHeader\": {\"resultCode\": \"200\"}, \"dataBody\": {\"v\": \"  10250\", \"r\": 1.50}}";

    private InsightSourcePort source;
    private KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper = JsonMapper.builder().build();
    private InsightTickListener listener;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        source = mock(InsightSourcePort.class);
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(source.dailyPrice(anyString())).thenReturn("  " + RAW + "\n");
        when(source.worldIndex(anyString())).thenReturn(RAW);
        when(source.exchangeRateSummary()).thenReturn(RAW);
        when(source.marketSummary()).thenReturn(RAW);
        listener = listener(List.of("TEST01", "TEST02"), List.of("C", "S"));
    }

    private InsightTickListener listener(List<String> symbols, List<String> continents) {
        var properties = new InsightProperties(new InsightProperties.Schedule(false, "0 30 16 * * MON-FRI"), "0", 30, continents);
        var publisher = new InsightPublisher(
                source, new CollectorProperties(symbols, 30000, 3_000), properties, kafka, mapper, CLOCK);
        return new InsightTickListener(publisher, mapper);
    }

    @Test
    void publishesOneEventPerResponseInOrderWithContractKeys() throws IOException {
        listener.onTick(tick("2026-10-20T16:30:00+09:00"));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(6)).send(eq(Topics.MARKET_INSIGHT), key.capture(), value.capture());
        assertThat(key.getAllValues()).containsExactly(
                "daily-price:TEST01", "daily-price:TEST02", "world-index:C", "world-index:S",
                "exchange-rate-summary", "market-summary");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
        }
    }

    @Test
    void eventCarriesTheRawResponseAndTickInformation() throws IOException {
        listener.onTick(tick("2026-10-20T16:30:00+09:00"));

        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(6)).send(eq(Topics.MARKET_INSIGHT), anyString(), value.capture());
        JsonNode daily = mapper.readTree(value.getAllValues().get(0));
        JsonNode summary = mapper.readTree(value.getAllValues().get(5));

        assertThat(daily.path("eventType").asString()).isEqualTo("market.insight");
        assertThat(daily.path("correlationId").asString()).isEqualTo("collect-post-close-2026-10-20T16:30:00+09:00");
        assertThat(daily.path("occurredAt").asString()).isEqualTo(daily.path("payload").path("fetchedAt").asString());
        assertThat(daily.path("payload").path("apiType").asString()).isEqualTo("daily-price");
        assertThat(daily.path("payload").path("target").asString()).isEqualTo("TEST01");
        // 바깥 공백만 제거되고 안쪽은 한 글자도 바뀌지 않는다.
        assertThat(daily.path("payload").path("response").asString()).isEqualTo(RAW);
        assertThat(daily.path("payload").path("scheduledFor").asString()).isEqualTo("2026-10-20T16:30:00+09:00");
        assertThat(daily.path("payload").path("fetchedAt").asString()).isEqualTo("2026-10-20T16:30:01+09:00");
        // 구분값이 없는 API 도 target 필드는 항상 있고 null 이다.
        assertThat(summary.path("payload").has("target")).isTrue();
        assertThat(summary.path("payload").path("target").isNull()).isTrue();
    }

    @Test
    void sameTickDeliveredTwiceIsProcessedOnce() throws IOException {
        listener.onTick(tick("2026-10-20T16:30:00+09:00"));
        listener.onTick(tick("2026-10-20T16:30:00+09:00"));

        verify(source, times(2)).dailyPrice(anyString());
        verify(source, times(1)).marketSummary();
        verify(kafka, times(6)).send(eq(Topics.MARKET_INSIGHT), anyString(), anyString());
    }

    @Test
    void laterTickIsProcessedAgain() throws IOException {
        listener.onTick(tick("2026-10-20T16:30:00+09:00"));
        listener.onTick(tick("2026-10-21T16:30:00+09:00"));

        verify(source, times(2)).marketSummary();
    }

    @Test
    void oneFailingApiDoesNotStopTheOthers() throws IOException {
        when(source.dailyPrice("TEST01")).thenThrow(new BrokerApiException(FailureType.HTTP_ERROR, "시세 API 오류 http=500 processCode=E777"));
        when(source.worldIndex("C")).thenThrow(new IllegalStateException("서버 문구가 섞인 예외"));
        when(source.exchangeRateSummary()).thenThrow(new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: IOException"));

        assertThatCode(() -> listener.onTick(tick("2026-10-20T16:30:00+09:00"))).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly("daily-price:TEST02", "world-index:S", "market-summary");
    }

    @Test
    void blankResponseIsNotPublished() throws IOException {
        when(source.marketSummary()).thenReturn("   ");

        listener.onTick(tick("2026-10-20T16:30:00+09:00"));

        verify(kafka, times(5)).send(eq(Topics.MARKET_INSIGHT), anyString(), anyString());
        verify(kafka, never()).send(anyString(), eq("market-summary"), anyString());
    }

    @Test
    void symbolOutsideTheContractIsSkippedWithoutBeingQueried() throws IOException {
        listener = listener(List.of("TEST01", "bad"), List.of());

        listener.onTick(tick("2026-10-20T16:30:00+09:00"));

        verify(source, never()).dailyPrice("bad");
        verify(source, times(1)).dailyPrice("TEST01");
    }

    @Test
    void ignoresOtherJobTypes() throws IOException {
        listener.onTick(ContractSchemas.sample("schedule.tick.sample.json"));
        listener.onTick(ContractSchemas.sample("schedule.tick.sample.json").replace("\"collect\"", "\"collect-news\""));

        verify(source, never()).marketSummary();
        verify(kafka, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    void usesItsOwnConsumerGroup() throws NoSuchMethodException, ClassNotFoundException {
        KafkaListener insight = InsightTickListener.class.getDeclaredMethod("onTick", String.class)
                .getAnnotation(KafkaListener.class);
        for (String other : List.of("TickListener", "NewsTickListener")) {
            KafkaListener otherListener = Class.forName("dev.stockreading.collector.schedule." + other)
                    .getDeclaredMethod("onTick", String.class).getAnnotation(KafkaListener.class);
            assertThat(insight.groupId()).isNotEqualTo(otherListener.groupId());
        }
    }

    private static String tick(String scheduledFor) throws IOException {
        return ContractSchemas.sample("schedule.tick.sample.json")
                .replace("\"collect\"", "\"collect-post-close\"")
                .replace("\"correlationId\": \"collect-2026-10-20T16:00:00+09:00\"",
                        "\"correlationId\": \"collect-post-close-2026-10-20T16:30:00+09:00\"")
                .replace("\"scheduledFor\": \"2026-10-20T16:00:00+09:00\"", "\"scheduledFor\": \"" + scheduledFor + "\"");
    }
}

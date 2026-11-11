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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.edgar.ExchangePort;
import dev.stockreading.collector.event.ContractSchemas;
import dev.stockreading.collector.holdings.HoldingsPort;
import dev.stockreading.collector.marketdata.BrokerApiException;
import dev.stockreading.collector.marketdata.FailureType;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class IntradayPublisherTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-21T01:05:01Z"), ZoneId.of("Asia/Seoul"));
    private static final String SCHEMA = "market.insight.v1.schema.json";
    private static final String RAW = "{\"dataHeader\": {\"resultCode\": \"200\"}, \"dataBody\": {\"v\": \"  1385.20\"}}";
    private static final long STAGGER_MS = 15_000;

    private InsightSourcePort source;
    private KafkaTemplate<String, String> kafka;
    private ListAppender<ILoggingEvent> logs;
    // 조회와 대기가 일어난 순서를 함께 기록해 4건이 대기 없이 연달아 나가지 않는지 본다.
    private final List<String> timeline = new ArrayList<>();
    private final ObjectMapper mapper = JsonMapper.builder().build();
    private InsightPublisher insight;
    private IntradayPublisher publisher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        source = mock(InsightSourcePort.class);
        kafka = mock(KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(source.exchangeRateSummary()).thenAnswer(call -> record("exchange-rate-summary"));
        when(source.marketSummary()).thenAnswer(call -> record("market-summary"));
        when(source.worldIndex(anyString())).thenAnswer(call -> record("world-index:" + call.getArgument(0)));
        HoldingsPort holdings = mock(HoldingsPort.class);
        when(holdings.allTickers()).thenReturn(List.of("TESTA"));
        var insightProperties = new InsightProperties(new InsightProperties.Schedule(false, "0 30 16 * * MON-FRI"), "0", 30,
                List.of("C", "S", "E"), 30, 100);
        insight = new InsightPublisher(source, new CollectorProperties(List.of("TEST01"), 30000), insightProperties,
                holdings, mock(ExchangePort.class), kafka, mapper, CLOCK);
        var intraday = new IntradayProperties(true, "0 */5 9-15 * * MON-FRI", "09:00", "15:30", STAGGER_MS);
        InsightCallPause pause = millis -> timeline.add("pause:" + millis);
        publisher = new IntradayPublisher(insight, source, intraday, pause);
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(InsightPublisher.class)).addAppender(logs);
        ((Logger) LoggerFactory.getLogger(IntradayPublisher.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(InsightPublisher.class)).detachAppender(logs);
        ((Logger) LoggerFactory.getLogger(IntradayPublisher.class)).detachAppender(logs);
    }

    private String record(String call) {
        timeline.add(call);
        return RAW;
    }

    @Test
    void intradayTickQueriesTheFourTargetsOneByOneWithAPauseBetween() throws IOException {
        intraday("2026-10-21T10:05:00+09:00");

        assertThat(timeline).containsExactly(
                "exchange-rate-summary", "pause:15000",
                "market-summary", "pause:15000",
                "world-index:S", "pause:15000",
                "world-index:C");
    }

    @Test
    void publishesOneMarketInsightPerTargetThatPassesTheContract() throws IOException {
        intraday("2026-10-21T10:05:00+09:00");

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(4)).send(eq(Topics.MARKET_INSIGHT), key.capture(), value.capture());
        assertThat(key.getAllValues()).containsExactly(
                "exchange-rate-summary", "market-summary", "world-index:S", "world-index:C");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
            JsonNode event = mapper.readTree(json);
            assertThat(event.path("correlationId").asString()).isEqualTo("collect-intraday-2026-10-21T10:05:00+09:00");
            assertThat(event.path("payload").path("scheduledFor").asString()).isEqualTo("2026-10-21T10:05:00+09:00");
            assertThat(event.path("payload").path("response").asString()).isEqualTo(RAW);
        }
        assertThat(messages()).anyMatch(m -> m.contains("sent 4 intraday market.insight events") && m.contains("failed=0"));
    }

    @Test
    void doesNotQueryDailyPricesOtherContinentsOrOverseasPrices() throws IOException {
        intraday("2026-10-21T10:05:00+09:00");

        verify(source, never()).dailyPrice(anyString());
        verify(source, never()).worldIndex("E");
        verify(source, never()).overseasDailyPrice(anyString(), anyString());
    }

    @Test
    void oneFailingTargetDoesNotStopTheOthersAndLogsNoServerText() throws IOException {
        when(source.exchangeRateSummary()).thenThrow(new IllegalStateException("서버가 보낸 문구 secret-token-value"));
        when(source.worldIndex("S"))
                .thenThrow(new BrokerApiException(FailureType.HTTP_ERROR, "시세 API 오류 http=500 processCode=E777"));
        // 다시 스텁할 때 Mockito 가 앞서 둔 응답을 한 번 불러 기록이 남으므로 지운다.
        timeline.clear();

        assertThatCode(() -> intraday("2026-10-21T10:05:00+09:00")).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly("market-summary", "world-index:C");
        // 실패한 조회 뒤에도 대기는 그대로 두어 다음 조회가 바로 붙지 않는다.
        assertThat(timeline).containsExactly(
                "pause:15000", "market-summary", "pause:15000", "pause:15000", "world-index:C");
        String all = String.join("\n", messages());
        assertThat(all).doesNotContain("서버가 보낸 문구", "secret-token-value");
        assertThat(all).contains("reason=IllegalStateException");
        assertThat(messages()).anyMatch(m -> m.contains("sent 2 intraday market.insight events") && m.contains("failed=2"));
    }

    @Test
    void rateLimitedTargetFailsOnlyThatTarget() throws IOException {
        when(source.marketSummary()).thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));

        intraday("2026-10-21T10:05:00+09:00");

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly("exchange-rate-summary", "world-index:S", "world-index:C");
    }

    @Test
    void blankResponseIsNotPublished() throws IOException {
        when(source.worldIndex("C")).thenReturn("  ");

        intraday("2026-10-21T10:05:00+09:00");

        verify(kafka, times(3)).send(eq(Topics.MARKET_INSIGHT), anyString(), anyString());
        verify(kafka, never()).send(anyString(), eq("world-index:C"), anyString());
    }

    private List<String> messages() {
        return logs.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private void intraday(String scheduledFor) {
        publisher.publishIntraday("collect-intraday-" + scheduledFor, scheduledFor);
    }
}

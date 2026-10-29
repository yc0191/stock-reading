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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.CollectorProperties;
import dev.stockreading.collector.Topics;
import dev.stockreading.collector.edgar.EdgarApiException;
import dev.stockreading.collector.edgar.EdgarFailure;
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

class InsightTickListenerTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:30:01Z"), ZoneId.of("Asia/Seoul"));
    private static final String SCHEMA = "market.insight.v1.schema.json";
    // 안쪽 공백과 숫자 표기가 그대로 남는지 보려고 일부러 어색하게 만든 가짜 응답이다.
    private static final String RAW = "{\"dataHeader\": {\"resultCode\": \"200\"}, \"dataBody\": {\"v\": \"  10250\", \"r\": 1.50}}";

    private InsightSourcePort source;
    private HoldingsPort holdings;
    private ExchangePort exchanges;
    private KafkaTemplate<String, String> kafka;
    private ListAppender<ILoggingEvent> logs;
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
        when(source.overseasDailyPrice(anyString(), anyString())).thenReturn(" " + RAW);
        // 보유 미국 종목이 없으면 기존 조회만 한다. 해외 일봉 테스트가 따로 목록을 준다.
        holdings = mock(HoldingsPort.class);
        when(holdings.allTickers()).thenReturn(List.of());
        exchanges = mock(ExchangePort.class);
        when(exchanges.exchanges(anyString())).thenReturn(List.of("Nasdaq"));
        listener = listener(List.of("TEST01", "TEST02"), List.of("C", "S"));
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(InsightPublisher.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(InsightPublisher.class)).detachAppender(logs);
    }

    private InsightTickListener listener(List<String> symbols, List<String> continents) {
        return listener(symbols, continents, 100);
    }

    private InsightTickListener listener(List<String> symbols, List<String> continents, int overseasMaxTickers) {
        var properties = new InsightProperties(new InsightProperties.Schedule(false, "0 30 16 * * MON-FRI"), "0", 30, continents,
                30, overseasMaxTickers);
        var publisher = new InsightPublisher(source, new CollectorProperties(symbols, 30000, 3_000), properties,
                holdings, exchanges, kafka, mapper, CLOCK);
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
    void rateLimitedExistingCallFailsOnlyThatCall() throws IOException {
        when(source.dailyPrice("TEST01")).thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));
        when(source.marketSummary()).thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));

        assertThatCode(() -> listener.onTick(tick("2026-10-20T16:30:00+09:00"))).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(4)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly(
                "daily-price:TEST02", "world-index:C", "world-index:S", "exchange-rate-summary");
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
    void postCloseTickNoLongerQueriesOverseasDailyPrices() throws IOException {
        // 해외 일봉은 장 시작 전 tick 에서만 받는다. 국내 장 마감 무렵에는 미국 장이 열리지 않아 같은 값을 다시 받게 된다.
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB.X"));

        listener.onTick(tick("2026-10-20T16:30:00+09:00"));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(6)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly(
                "daily-price:TEST01", "daily-price:TEST02", "world-index:C", "world-index:S",
                "exchange-rate-summary", "market-summary");
        verify(source, never()).overseasDailyPrice(anyString(), anyString());
        verify(exchanges, never()).exchanges(anyString());
    }

    @Test
    void tickersOverTheLimitAreNotQueriedAndOnlyTheirCountIsLogged() throws IOException {
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB", "TESTC"));
        listener = listener(List.of(), List.of(), 2);

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));

        verify(source).overseasDailyPrice("NAS", "TESTA");
        verify(source).overseasDailyPrice("NAS", "TESTB");
        verify(source, never()).overseasDailyPrice(anyString(), eq("TESTC"));
        verify(exchanges, never()).exchanges("TESTC");
        assertThat(messages()).anyMatch(m -> m.contains("skipped 1 tickers over the limit 2"));
        assertThat(String.join("\n", messages())).doesNotContain("TESTA", "TESTB", "TESTC");
    }

    @Test
    void oneFailingOverseasTickerDoesNotStopTheOthersAndIsLoggedByPositionAndKind() throws IOException {
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB", "TESTC", "TESTD", "TESTE"));
        when(exchanges.exchanges("TESTA")).thenThrow(new EdgarApiException(EdgarFailure.TIMEOUT, null));
        when(exchanges.exchanges("TESTB")).thenReturn(List.of("CBOE"));
        when(source.overseasDailyPrice("NAS", "TESTC"))
                .thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));
        when(source.overseasDailyPrice("NAS", "TESTD"))
                .thenThrow(new BrokerApiException(FailureType.HTTP_ERROR, "시세 API 오류 http=500 processCode=E777"));

        assertThatCode(() -> listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"))).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(2)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        // 세계지수는 그대로 나가고, 해외 일봉은 실패하지 않은 마지막 종목만 나간다.
        assertThat(key.getAllValues()).containsExactly("world-index:C", "overseas-daily-price:TESTE");
        List<String> messages = messages();
        assertThat(messages).anyMatch(m -> m.contains("position=1 reason=EXCHANGE_LOOKUP_FAILED"));
        assertThat(messages).anyMatch(m -> m.contains("position=2 reason=EXCHANGE_UNKNOWN"));
        assertThat(messages).anyMatch(m -> m.contains("position=3 reason=TICK_SKIPPED"));
        assertThat(messages).anyMatch(m -> m.contains("position=4 reason=HTTP_ERROR"));
        assertThat(messages).anyMatch(m -> m.contains("sent 1 overseas daily price events")
                && m.contains("tickers=5 failed=4")
                && m.contains("EXCHANGE_LOOKUP_FAILED=1") && m.contains("EXCHANGE_UNKNOWN=1")
                && m.contains("HTTP_ERROR=1") && m.contains("TICK_SKIPPED=1"));
        assertThat(String.join("\n", messages)).doesNotContain("TESTA", "TESTB", "TESTC", "TESTD", "TESTE");
    }

    @Test
    void blankOverseasResponseIsCountedAsAFailure() throws IOException {
        when(holdings.allTickers()).thenReturn(List.of("TESTA"));
        when(source.overseasDailyPrice("NAS", "TESTA")).thenReturn("  ");
        listener = listener(List.of(), List.of());

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));

        verify(kafka, never()).send(anyString(), eq("overseas-daily-price:TESTA"), anyString());
        assertThat(messages()).anyMatch(m -> m.contains("position=1 reason=EMPTY_RESPONSE"));
    }

    @Test
    void preOpenTickQueriesOnlyTheAmericasIndexAndHoldingsDailyPrices() throws IOException {
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB.X"));
        when(exchanges.exchanges("TESTB.X")).thenReturn(List.of("NYSE"));

        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_INSIGHT), key.capture(), value.capture());
        // 이벤트와 키는 장 마감 후 조회와 같고, 국내 일봉·다른 대륙·환율·시장종합은 조회하지 않는다.
        assertThat(key.getAllValues()).containsExactly(
                "world-index:C", "overseas-daily-price:TESTA", "overseas-daily-price:TESTB.X");
        verify(source).worldIndex("C");
        verify(source, never()).worldIndex("S");
        verify(source, never()).dailyPrice(anyString());
        verify(source, never()).exchangeRateSummary();
        verify(source, never()).marketSummary();
        verify(source).overseasDailyPrice("NAS", "TESTA");
        verify(source).overseasDailyPrice("NYS", "TESTB.X");
        for (String json : value.getAllValues()) {
            assertThat(ContractSchemas.validate(SCHEMA, json)).isEmpty();
            JsonNode event = mapper.readTree(json);
            assertThat(event.path("correlationId").asString()).isEqualTo("collect-pre-open-2026-10-21T08:30:00+09:00");
            assertThat(event.path("payload").path("scheduledFor").asString()).isEqualTo("2026-10-21T08:30:00+09:00");
        }
    }

    @Test
    void preOpenIndexFailureAndOneTickerFailureDoNotStopTheRestAndLogNoTickers() throws IOException {
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB"));
        when(source.worldIndex("C")).thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));
        when(source.overseasDailyPrice("NAS", "TESTA"))
                .thenThrow(new BrokerApiException(FailureType.HTTP_ERROR, "시세 API 오류 http=500 processCode=E777"));

        assertThatCode(() -> listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"))).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(1)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly("overseas-daily-price:TESTB");
        List<String> messages = messages();
        assertThat(messages).anyMatch(m -> m.contains("sent 0 pre-open world index events") && m.contains("failed=1"));
        assertThat(messages).anyMatch(m -> m.contains("position=1 reason=HTTP_ERROR"));
        assertThat(String.join("\n", messages)).doesNotContain("TESTA", "TESTB");
    }

    @Test
    void preOpenAndPostCloseTicksOfTheSameDayAreBothProcessed() throws IOException {
        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));
        listener.onTick(tick("2026-10-21T16:30:00+09:00"));
        listener.onTick(preOpenTick("2026-10-21T08:30:00+09:00"));

        verify(source, times(2)).worldIndex("C");
        verify(source, times(1)).marketSummary();
    }

    private List<String> messages() {
        return logs.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
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

    private static String preOpenTick(String scheduledFor) throws IOException {
        return ContractSchemas.sample("schedule.tick.sample.json")
                .replace("\"collect\"", "\"collect-pre-open\"")
                .replace("\"correlationId\": \"collect-2026-10-20T16:00:00+09:00\"",
                        "\"correlationId\": \"collect-pre-open-2026-10-21T08:30:00+09:00\"")
                .replace("\"scheduledFor\": \"2026-10-20T16:00:00+09:00\"", "\"scheduledFor\": \"" + scheduledFor + "\"");
    }

    private static String tick(String scheduledFor) throws IOException {
        return ContractSchemas.sample("schedule.tick.sample.json")
                .replace("\"collect\"", "\"collect-post-close\"")
                .replace("\"correlationId\": \"collect-2026-10-20T16:00:00+09:00\"",
                        "\"correlationId\": \"collect-post-close-2026-10-20T16:30:00+09:00\"")
                .replace("\"scheduledFor\": \"2026-10-20T16:00:00+09:00\"", "\"scheduledFor\": \"" + scheduledFor + "\"");
    }
}

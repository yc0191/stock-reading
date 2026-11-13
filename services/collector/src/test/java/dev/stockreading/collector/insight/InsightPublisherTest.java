package dev.stockreading.collector.insight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.collector.holdings.QuoteSymbolsPort;
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

class InsightPublisherTest {

    private static final Instant START = Instant.parse("2026-10-20T07:30:01Z");
    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    // 기본 설정과 같은 재시도: 6분 간격 최대 3번.
    private static final InsightProperties.Retry RETRY = new InsightProperties.Retry(3, Duration.ofMinutes(6));
    private static final String SCHEMA = "market.insight.v1.schema.json";
    // 안쪽 공백과 숫자 표기가 그대로 남는지 보려고 일부러 어색하게 만든 가짜 응답이다.
    private static final String RAW = "{\"dataHeader\": {\"resultCode\": \"200\"}, \"dataBody\": {\"v\": \"  10250\", \"r\": 1.50}}";

    private InsightSourcePort source;
    private HoldingsPort holdings;
    private ExchangePort exchanges;
    private KafkaTemplate<String, String> kafka;
    private ListAppender<ILoggingEvent> logs;
    private final ObjectMapper mapper = JsonMapper.builder().build();
    private InsightPublisher publisher;
    // 재시도 대기는 실제로 기다리지 않고 시계만 그만큼 앞으로 옮긴다.
    private MutableClock clock;
    private final List<Long> pauses = new ArrayList<>();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        clock = new MutableClock(START);
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
        publisher = publisher(List.of("TEST01", "TEST02"), List.of("C", "S"));
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(InsightPublisher.class)).addAppender(logs);
    }

    @AfterEach
    void detachLogs() {
        ((Logger) LoggerFactory.getLogger(InsightPublisher.class)).detachAppender(logs);
    }

    private InsightPublisher publisher(List<String> symbols, List<String> continents) {
        return publisher(symbols, continents, 100);
    }

    private InsightPublisher publisher(List<String> symbols, List<String> continents, int overseasMaxTickers) {
        return publisher(symbols, continents, overseasMaxTickers, RETRY);
    }

    private InsightPublisher publisher(List<String> symbols, List<String> continents, int overseasMaxTickers,
                                       InsightProperties.Retry retry) {
        var properties = new InsightProperties(new InsightProperties.Schedule(false, "0 30 16 * * MON-FRI"), "0", 30, continents,
                30, overseasMaxTickers, retry);
        InsightCallPause pause = millis -> {
            pauses.add(millis);
            clock.now = clock.now.plusMillis(Math.max(0, millis));
        };
        return new InsightPublisher(source, (QuoteSymbolsPort) () -> symbols, properties,
                holdings, exchanges, kafka, mapper, clock, pause);
    }

    @Test
    void publishesOneEventPerResponseInOrderWithContractKeys() throws IOException {
        postClose("2026-10-20T16:30:00+09:00");

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
        postClose("2026-10-20T16:30:00+09:00");

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
    void oneFailingApiDoesNotStopTheOthers() throws IOException {
        when(source.dailyPrice("TEST01")).thenThrow(new BrokerApiException(FailureType.HTTP_ERROR, "시세 API 오류 http=500 processCode=E777"));
        when(source.worldIndex("C")).thenThrow(new IllegalStateException("서버 문구가 섞인 예외"));
        when(source.exchangeRateSummary()).thenThrow(new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: IOException"));

        assertThatCode(() -> postClose("2026-10-20T16:30:00+09:00")).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(3)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly("daily-price:TEST02", "world-index:S", "market-summary");
    }

    @Test
    void rateLimitedExistingCallFailsOnlyThatCall() throws IOException {
        when(source.dailyPrice("TEST01")).thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));
        when(source.marketSummary()).thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));

        assertThatCode(() -> postClose("2026-10-20T16:30:00+09:00")).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(4)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly(
                "daily-price:TEST02", "world-index:C", "world-index:S", "exchange-rate-summary");
    }

    @Test
    void blankResponseIsNotPublished() throws IOException {
        when(source.marketSummary()).thenReturn("   ");

        postClose("2026-10-20T16:30:00+09:00");

        verify(kafka, times(5)).send(eq(Topics.MARKET_INSIGHT), anyString(), anyString());
        verify(kafka, never()).send(anyString(), eq("market-summary"), anyString());
    }

    @Test
    void symbolOutsideTheContractIsSkippedWithoutBeingQueried() throws IOException {
        publisher = publisher(List.of("TEST01", "bad"), List.of());

        postClose("2026-10-20T16:30:00+09:00");

        verify(source, never()).dailyPrice("bad");
        verify(source, times(1)).dailyPrice("TEST01");
    }

    @Test
    void postCloseTickNoLongerQueriesOverseasDailyPrices() throws IOException {
        // 해외 일봉은 장 시작 전 tick 에서만 받는다. 국내 장 마감 무렵에는 미국 장이 열리지 않아 같은 값을 다시 받게 된다.
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB.X"));

        postClose("2026-10-20T16:30:00+09:00");

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
        publisher = publisher(List.of(), List.of(), 2);

        preOpen("2026-10-21T08:30:00+09:00");

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

        assertThatCode(() -> preOpen("2026-10-21T08:30:00+09:00")).doesNotThrowAnyException();

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
        publisher = publisher(List.of(), List.of());

        preOpen("2026-10-21T08:30:00+09:00");

        verify(kafka, never()).send(anyString(), eq("overseas-daily-price:TESTA"), anyString());
        assertThat(messages()).anyMatch(m -> m.contains("position=1 reason=EMPTY_RESPONSE"));
    }

    @Test
    void preOpenTickQueriesOnlyTheAmericasIndexAndHoldingsDailyPrices() throws IOException {
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB.X"));
        when(exchanges.exchanges("TESTB.X")).thenReturn(List.of("NYSE"));

        preOpen("2026-10-21T08:30:00+09:00");

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

        assertThatCode(() -> preOpen("2026-10-21T08:30:00+09:00")).doesNotThrowAnyException();

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kafka, times(1)).send(eq(Topics.MARKET_INSIGHT), key.capture(), anyString());
        assertThat(key.getAllValues()).containsExactly("overseas-daily-price:TESTB");
        List<String> messages = messages();
        assertThat(messages).anyMatch(m -> m.contains("sent 0 pre-open world index events") && m.contains("failed=1"));
        assertThat(messages).anyMatch(m -> m.contains("position=1 reason=HTTP_ERROR"));
        assertThat(String.join("\n", messages)).doesNotContain("TESTA", "TESTB");
    }

    @Test
    void postCloseReturnsTheKindsItCouldNotGetForTheDailyNotice() {
        when(source.dailyPrice("TEST01")).thenThrow(new BrokerApiException(FailureType.HTTP_ERROR, "시세 API 오류 http=500"));
        when(source.marketSummary()).thenReturn("  ");

        assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00"))
                .containsExactly("daily-price", "market-summary");
        assertThat(publisher(List.of("TEST01"), List.of("C")).publishAll("corr", "2026-10-20T16:30:00+09:00"))
                .containsExactly("daily-price", "market-summary");
    }

    // #64 재현: 마감 후 조회가 연결 실패하면 다시 부르지 않아 그날 일봉을 못 받은 채 끝났다.
    @Test
    void postCloseCallThatFailedToConnectIsCalledAgainAndCountsAsReceived() {
        when(source.dailyPrice("TEST01"))
                .thenThrow(new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: HttpConnectTimeoutException"))
                .thenReturn(RAW);

        assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00")).isEmpty();
        verify(source, times(2)).dailyPrice("TEST01");
        verify(source, times(1)).dailyPrice("TEST02");
    }

    @Test
    void onlyFailedPostCloseCallsAreRetriedSixMinutesApartUpToThreeTimes() {
        List<String> calledAt = new ArrayList<>();
        when(source.dailyPrice("TEST01")).thenAnswer(call -> {
            calledAt.add(OffsetDateTime.now(clock).toLocalTime().toString());
            throw new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: HttpConnectTimeoutException");
        });
        when(source.marketSummary()).thenThrow(new BrokerApiException(FailureType.TIMEOUT, "시세 API 호출 실패: HttpTimeoutException"));

        assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00"))
                .containsExactly("daily-price", "market-summary");

        // 첫 조회 16:30:01, 회차 시작 시각에서 6분 배수 시각에 3번 더.
        assertThat(calledAt).containsExactly("16:30:01", "16:36:01", "16:42:01", "16:48:01");
        assertThat(pauses).containsExactly(360_000L, 360_000L, 360_000L);
        verify(source, times(4)).marketSummary();
        // 성공한 조회는 다시 부르지 않는다.
        verify(source, times(1)).dailyPrice("TEST02");
        verify(source, times(1)).worldIndex("C");
        verify(source, times(1)).exchangeRateSummary();
        assertThat(messages()).containsSubsequence(
                "sent 4 market.insight events corr=corr failed=2",
                "post-close retry attempt=1/3 corr=corr retried=2 published=0 failed=2",
                "post-close retry attempt=2/3 corr=corr retried=2 published=0 failed=2",
                "post-close retry attempt=3/3 corr=corr retried=2 published=0 failed=2");
    }

    @Test
    void retryStopsOnceEverythingIsReceivedAndTheRetriedEventKeepsTheRoundTime() throws IOException {
        when(source.worldIndex("C"))
                .thenThrow(new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: HttpConnectTimeoutException"))
                .thenThrow(new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: HttpConnectTimeoutException"))
                .thenReturn(RAW);

        assertThat(publisher.publishAll("collect-post-close-2026-10-20T16:30:00+09:00", "2026-10-20T16:30:00+09:00"))
                .isEmpty();

        assertThat(pauses).hasSize(2);
        verify(source, times(3)).worldIndex("C");
        assertThat(messages()).contains(
                "post-close retry attempt=2/3 corr=collect-post-close-2026-10-20T16:30:00+09:00 retried=1 published=1 failed=0");
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq(Topics.MARKET_INSIGHT), eq("world-index:C"), value.capture());
        JsonNode retried = mapper.readTree(value.getValue());
        // 다시 받은 응답도 같은 회차(scheduledFor·correlationId)이고, 받은 시각은 실제로 받은 16:42 이다.
        assertThat(retried.path("payload").path("scheduledFor").asString()).isEqualTo("2026-10-20T16:30:00+09:00");
        assertThat(retried.path("payload").path("fetchedAt").asString()).isEqualTo("2026-10-20T16:42:01+09:00");
        assertThat(ContractSchemas.validate(SCHEMA, value.getValue())).isEmpty();
    }

    @Test
    void aSlowFirstRoundDoesNotPushTheRetriesLater() {
        when(source.marketSummary())
                .thenAnswer(call -> {
                    // 첫 회차가 연결 시간 초과로 7분 걸렸다.
                    clock.now = clock.now.plus(Duration.ofMinutes(7));
                    throw new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: HttpConnectTimeoutException");
                })
                .thenReturn(RAW);

        assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00")).isEmpty();

        // 16:36:01 이 이미 지났으므로 기다리지 않고 바로 다시 부른다.
        assertThat(pauses).singleElement().satisfies(millis -> assertThat(millis).isLessThanOrEqualTo(0L));
    }

    @Test
    void zeroRetriesMeansTheFirstRoundIsFinal() {
        when(source.marketSummary()).thenThrow(new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: IOException"));
        publisher = publisher(List.of("TEST01"), List.of(), 100, new InsightProperties.Retry(0, Duration.ofMinutes(6)));

        assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00")).containsExactly("market-summary");
        verify(source, times(1)).marketSummary();
        assertThat(pauses).isEmpty();
    }

    @Test
    void anInvalidSymbolIsNotRetried() {
        publisher = publisher(List.of("TEST01", "bad"), List.of());

        assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00")).containsExactly("daily-price");
        assertThat(pauses).isEmpty();
        verify(source, never()).dailyPrice("bad");
    }

    @Test
    void anInterruptStopsTheRemainingRetries() {
        when(source.marketSummary()).thenThrow(new BrokerApiException(FailureType.CONNECTION_FAILED, "시세 API 호출 실패: IOException"));
        var properties = new InsightProperties(new InsightProperties.Schedule(false, "0 30 16 * * MON-FRI"), "0", 30,
                List.of(), 30, 100, RETRY);
        InsightCallPause interrupted = millis -> Thread.currentThread().interrupt();
        publisher = new InsightPublisher(source, (QuoteSymbolsPort) () -> List.of(), properties, holdings, exchanges,
                kafka, mapper, clock, interrupted);
        try {
            assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00")).containsExactly("market-summary");
            verify(source, times(1)).marketSummary();
        } finally {
            // 테스트 스레드의 중단 표시를 지운다.
            Thread.interrupted();
        }
    }

    @Test
    void retrySettingsDefaultToThreeTimesSixMinutesApart() {
        var properties = new InsightProperties(new InsightProperties.Schedule(false, "0 30 16 * * MON-FRI"), "0", 30,
                List.of(), 30, 100, null);

        assertThat(properties.retry()).isEqualTo(new InsightProperties.Retry(3, Duration.ofMinutes(6)));
    }

    @EnableConfigurationProperties(InsightProperties.class)
    static class RetryWiring {
    }

    @Test
    void applicationSettingsRetryThreeTimesSixMinutesApartAndTheEnvironmentCanChangeIt() {
        // application.yml 을 읽어 기본값과 환경변수 연결을 함께 확인한다.
        var runner = new ApplicationContextRunner()
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .withUserConfiguration(RetryWiring.class);

        runner.run(context -> assertThat(context.getBean(InsightProperties.class).retry())
                .isEqualTo(new InsightProperties.Retry(3, Duration.ofMinutes(6))));
        runner.withPropertyValues("COLLECTOR_INSIGHT_RETRY_MAX_ATTEMPTS=0", "COLLECTOR_INSIGHT_RETRY_INTERVAL=90s")
                .run(context -> assertThat(context.getBean(InsightProperties.class).retry())
                        .isEqualTo(new InsightProperties.Retry(0, Duration.ofSeconds(90))));
    }

    @Test
    void retrySettingsRejectNegativeCountsAndEmptyIntervals() {
        assertThatThrownBy(() -> new InsightProperties.Retry(-1, Duration.ofMinutes(6)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InsightProperties.Retry(3, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new InsightProperties.Retry(3, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void postCloseThatGotEverythingReturnsNoMissingKind() {
        assertThat(publisher.publishAll("corr", "2026-10-20T16:30:00+09:00")).isEmpty();
    }

    @Test
    void preOpenReturnsTheKindsItCouldNotGetIncludingTickersSkippedOverTheLimit() {
        when(holdings.allTickers()).thenReturn(List.of("TESTA", "TESTB"));

        assertThat(publisher(List.of(), List.of(), 2).publishPreOpen("corr", "2026-10-21T08:30:00+09:00")).isEmpty();
        assertThat(publisher(List.of(), List.of(), 1).publishPreOpen("corr", "2026-10-21T08:30:00+09:00"))
                .containsExactly("overseas-daily-price");
        when(source.worldIndex("C")).thenThrow(new BrokerApiException(FailureType.TICK_SKIPPED, "호출 상한 대기 시간 초과"));
        when(source.overseasDailyPrice("NAS", "TESTB")).thenReturn(" ");
        assertThat(publisher.publishPreOpen("corr", "2026-10-21T08:30:00+09:00"))
                .containsExactly("world-index", "overseas-daily-price");
    }

    private List<String> messages() {
        return logs.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    private void preOpen(String scheduledFor) {
        publisher.publishPreOpen("collect-pre-open-2026-10-21T08:30:00+09:00", scheduledFor);
    }

    private void postClose(String scheduledFor) {
        publisher.publishAll("collect-post-close-2026-10-20T16:30:00+09:00", scheduledFor);
    }

    private static final class MutableClock extends Clock {
        Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        @Override
        public ZoneId getZone() {
            return KST;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}

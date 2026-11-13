package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.market.MarketProperties;
import dev.stockreading.market.api.InvalidRequestHandler;
import dev.stockreading.market.constituent.EtfInfo;
import dev.stockreading.market.constituent.HoldingsPort;
import dev.stockreading.market.event.DailyPayload;
import dev.stockreading.market.insight.OverseasCloseRate;
import dev.stockreading.market.insight.OverseasCloseRateQuery;
import dev.stockreading.market.members.MemberChangesPort;
import dev.stockreading.market.trace.CorrelationId;
import dev.stockreading.market.trace.CorrelationIdHttpFilter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * 시장 배경·데일리 시세의 내부 조회 API 가 조회 클래스의 답을 그대로 JSON 으로 돌려주는지(요청 검사, 추적 ID 포함)
 * 지킨다. 저장소는 대역이고 값은 모두 가짜다.
 */
class ContextQueryControllerTest {

    private static final String CORRELATION_HEADER = "X-Correlation-Id";
    private static final String CORRELATION_ID = "move-2026-10-20T16:00:31+09:00";
    private static final OffsetDateTime CONTEXT_AS_OF = OffsetDateTime.parse("2026-10-20T16:00:31+09:00");
    private static final OffsetDateTime CONTEXT_SINCE = OffsetDateTime.parse("2026-10-16T16:00:31+09:00");
    private static final OffsetDateTime DAILY_AS_OF = OffsetDateTime.parse("2026-10-20T17:00:00+09:00");
    private static final OffsetDateTime DAILY_SINCE = OffsetDateTime.parse("2026-10-16T17:00:00+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T08:15:43Z"), ZoneId.of("Asia/Seoul"));

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final Logger controllerLog = (Logger) LoggerFactory.getLogger(ContextQueryController.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    private BackgroundRepository repository;
    private OverseasCloseRateQuery closeRateQuery;
    private HoldingsPort holdings;
    private MemberChangesPort memberChanges;
    private OperationsReader operationsReader;
    private OpenFlowPort openFlowPort;
    private ContextQuery contextListener;
    private DailyQuery dailyListener;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        repository = mock(BackgroundRepository.class);
        closeRateQuery = mock(OverseasCloseRateQuery.class);
        holdings = mock(HoldingsPort.class);
        memberChanges = mock(MemberChangesPort.class);
        operationsReader = mock(OperationsReader.class);
        openFlowPort = mock(OpenFlowPort.class);
        givenStoredData();

        ContextProperties properties =
                new ContextProperties(Duration.ofDays(4), 32_768, 100, List.of("C", "S"), Duration.ofDays(7));
        contextListener = new ContextQuery(repository, new WorldIndexRates(mapper), closeRateQuery, holdings,
                properties);
        dailyListener = new DailyQuery(repository, new WorldIndexRates(mapper), closeRateQuery, holdings,
                memberChanges, properties, new DailyProperties(50, Duration.ofMinutes(30)), operationsReader,
                mapper, openFlowPort, (symbol, since, asOf) -> Optional.empty(),
                new MarketProperties(5, BigDecimal.ONE, List.of("f.a", "f.b", "f.c"), BigDecimal.ONE,
                        List.of("f.open"), 1, List.of(), 0, new BigDecimal("2.00")));
        mvc = MockMvcBuilders.standaloneSetup(new ContextQueryController(contextListener, dailyListener))
                .setControllerAdvice(new InvalidRequestHandler())
                .addFilters(new CorrelationIdHttpFilter())
                .setMessageConverters(new JacksonJsonHttpMessageConverter(mapper))
                .build();

        appender.start();
        controllerLog.addAppender(appender);
    }

    @AfterEach
    void detach() {
        controllerLog.detachAppender(appender);
    }

    /** 응답의 칸이 되도록 많이 차도록 자료를 넣는다. 빈 응답끼리 같아서 통과하는 일이 없게 한다. */
    private void givenStoredData() {
        when(repository.latest(any(), any(), any(), any())).thenReturn(Optional.empty());
        when(repository.latest(eq(BackgroundKind.EXCHANGE_RATE_SUMMARY), any(), any(), any())).thenReturn(Optional.of(
                new BackgroundRepository.StoredResponse("{\"fx\": \"1380.5\"}", CONTEXT_AS_OF.minusHours(1))));
        when(repository.worldIndexRows(anyString(), any(), any())).thenReturn(List.of());
        when(repository.latestDailyPrices(any(), any())).thenReturn(List.of(
                new BackgroundRepository.StoredTargetResponse("TEST01", new BackgroundRepository.StoredResponse(
                        DailyPriceReaderTest.body(
                                DailyPriceReaderTest.record("20261019", "10000", "9900", "1.01"),
                                DailyPriceReaderTest.record("20261020", " 9850", " 10000", "  -1.50")),
                        DAILY_AS_OF.minusMinutes(30)))));
        when(holdings.trackedEtfs()).thenReturn(List.of("TEST01", "TEST09"));
        when(holdings.topTickers(anyString())).thenReturn(List.of());
        when(holdings.topTickers("TEST01")).thenReturn(List.of("FAKEA", "FAKEB"));
        when(holdings.stockName(anyString())).thenReturn(Optional.empty());
        when(holdings.etfInfo(anyString())).thenReturn(Optional.empty());
        when(holdings.etfInfo("TEST01")).thenReturn(Optional.of(new EtfInfo("TEST ETF", "TEST INDEX", null, List.of(
                new EtfInfo.Member("US", "FAKEA", "FAKE CORP", new BigDecimal("12.500"), 1),
                new EtfInfo.Member("KR", "TEST09", null, null, 2)))));
        when(closeRateQuery.find(anyString(), any(), any())).thenReturn(Optional.empty());
        when(closeRateQuery.find(eq("FAKEA"), any(), any())).thenReturn(Optional.of(new OverseasCloseRate(
                "FAKEA", "20261019", OffsetDateTime.parse("2026-10-19T16:00:00-04:00"), "-0.75",
                OffsetDateTime.parse("2026-10-19T23:30:02Z"))));
        when(memberChanges.changes(anyString(), any(), any())).thenReturn(Optional.empty());
        when(operationsReader.read(any(), any())).thenReturn(new DailyPayload.Operations(
                List.of(new DailyPayload.CollectionStatus("quote-1s", null, null, 0)), Map.of("TIMEOUT", 2),
                List.of()));
    }

    private static String sample(String file) throws IOException {
        return ContractSchemas.sample(file);
    }

    // 같은 요청 본문을 조회 클래스에 직접 넣어 얻은 답
    private <T> T requestBody(String requestEnvelope, Class<T> type) {
        return mapper.treeToValue(mapper.readTree(requestEnvelope).get("payload"), type);
    }

    private JsonNode apiPayload(String path, String requestEnvelope) throws Exception {
        String body = mapper.readTree(requestEnvelope).get("payload").toString();
        String response = mvc.perform(post(path).contentType(MediaType.APPLICATION_JSON)
                        .header(CORRELATION_HEADER, CORRELATION_ID).content(body))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(response);
    }

    @Test
    void contextApiAnswersTheSameContentAsTheQueryAnswer() throws Exception {
        String request = sample("market.context.requested.sample.json");

        JsonNode answer = mapper.readTree(mapper.writeValueAsString(contextListener.answer(requestBody(request, ContextRequest.class))));
        JsonNode api = apiPayload("/internal/market/context", request);

        assertThat(api).isEqualTo(answer);
        // 비교가 빈 응답끼리가 아님을 확인한다.
        assertThat(api.get("tickers").size()).isEqualTo(2);
        assertThat(api.get("closeRates").size()).isEqualTo(1);
        assertThat(api.get("etf").get("name").asString()).isEqualTo("TEST ETF");
    }

    @Test
    void dailyApiAnswersTheSameContentAsTheQueryAnswerIncludingOpenFlow() throws Exception {
        when(openFlowPort.first(anyString(), any(), any())).thenReturn(Optional.of(new OpenFlowPort.Snapshot(
                DAILY_AS_OF.minusMinutes(10), 10_100, null)));
        when(openFlowPort.last(anyString(), any(), any())).thenReturn(Optional.of(new OpenFlowPort.Snapshot(
                DAILY_AS_OF.minusSeconds(2), 10_300, "{\"f\":{\"open\":10200,\"a\":10000,\"b\":10000,\"c\":10000}}")));
        String request = sample("market.daily.requested.sample.json").replace("\"maxTickers\": 100",
                "\"maxTickers\": 100,\n    \"openFlowSince\": \"" + DAILY_AS_OF.minusMinutes(10) + "\"");

        JsonNode answer = mapper.readTree(mapper.writeValueAsString(dailyListener.answer(requestBody(request, DailyRequest.class))));
        JsonNode api = apiPayload("/internal/market/daily", request);

        assertThat(api).isEqualTo(answer);
        assertThat(api.get("etfs").size()).isEqualTo(2);
        assertThat(api.get("etfs").get(0).get("rate").asString()).isEqualTo("-1.50");
        assertThat(api.get("etfs").get(1).get("missingReason").asString()).isEqualTo(DailyPayload.NO_DAILY_PRICE_IN_RANGE);
        assertThat(api.get("etfs").get(0).has("openFlow")).isTrue();
        assertThat(api.get("operations").get("quoteFailures").get("TIMEOUT").asInt()).isEqualTo(2);
    }

    @Test
    void theApiLogLineCarriesTheRequestCorrelationIdHeader() throws Exception {
        apiPayload("/internal/market/context", sample("market.context.requested.sample.json"));

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getMDCPropertyMap()).containsEntry(CorrelationId.KEY, CORRELATION_ID);
    }

    @Test
    void anInvalidRequestIsABadRequestNotAServerErrorSoTheCallerDoesNotRetryIt() throws Exception {
        String reversed = mapper.readTree(sample("market.context.requested.sample.json")).get("payload").toString()
                .replace("\"since\":\"2026-10-16T16:00:31+09:00\"", "\"since\":\"2026-10-21T16:00:31+09:00\"");
        String unreadableTime = mapper.readTree(sample("market.daily.requested.sample.json")).get("payload").toString()
                .replace("\"asOf\":\"2026-10-20T17:00:00+09:00\"", "\"asOf\":\"not-a-time\"");

        mvc.perform(post("/internal/market/context").contentType(MediaType.APPLICATION_JSON).content(reversed))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/internal/market/daily").contentType(MediaType.APPLICATION_JSON).content(unreadableTime))
                .andExpect(status().isBadRequest());
    }

    private String payloadWithout(String file, String field) throws IOException {
        ObjectNode payload = (ObjectNode) mapper.readTree(sample(file)).get("payload");
        payload.remove(field);
        return payload.toString();
    }

    @Test
    void aRequestMissingARequiredFieldIsABadRequestNotAServerError() throws Exception {
        mvc.perform(post("/internal/market/context").contentType(MediaType.APPLICATION_JSON)
                        .content(payloadWithout("market.context.requested.sample.json", "since")))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/internal/market/daily").contentType(MediaType.APPLICATION_JSON)
                        .content(payloadWithout("market.daily.requested.sample.json", "asOf")))
                .andExpect(status().isBadRequest());
    }
}

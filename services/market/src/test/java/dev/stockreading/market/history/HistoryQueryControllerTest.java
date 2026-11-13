package dev.stockreading.market.history;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.IntStream;

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
import dev.stockreading.market.api.InvalidRequestHandler;
import dev.stockreading.market.trace.CorrelationId;
import dev.stockreading.market.trace.CorrelationIdHttpFilter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * 가격 이력의 내부 조회 API 가 조회 클래스의 답을 그대로 JSON 으로 돌려주는지(요청 검사, 추적 ID 포함) 지킨다.
 * 저장소는 대역이고 값은 모두 가짜다.
 */
class HistoryQueryControllerTest {

    private static final String CORRELATION_ID = "move-2026-10-20T16:00:31+09:00";
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-20T15:55:31+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T16:10:31+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:43Z"), ZoneId.of("Asia/Seoul"));

    private final JsonMapper mapper = JsonMapper.builder().build();
    private final PriceHistoryRepository repository = mock(PriceHistoryRepository.class);
    private final Logger controllerLog = (Logger) LoggerFactory.getLogger(HistoryQueryController.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private HistoryQuery listener;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        listener = new HistoryQuery(repository);
        mvc = MockMvcBuilders.standaloneSetup(new HistoryQueryController(listener))
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

    private static List<PriceHistoryRepository.StoredPoint> rows(int count) {
        // DB 드라이버처럼 UTC 로 돌려줘, 응답이 KST 로 바꿔 쓰는 것까지 같이 비교한다.
        return IntStream.range(0, count)
                .mapToObj(i -> new PriceHistoryRepository.StoredPoint(
                        SINCE.plusSeconds(10L * i).withOffsetSameInstant(java.time.ZoneOffset.UTC), 10_000L + i))
                .toList();
    }

    private String request(String extra) throws Exception {
        return ContractSchemas.sample("market.history.requested.sample.json").replace("\"maxPoints\": 5000", extra);
    }

    // 같은 요청 본문으로 조회 클래스를 직접 불러 얻은 답
    private JsonNode directAnswer(String requestEnvelope) {
        return mapper.readTree(mapper.writeValueAsString(listener.answer(
                mapper.treeToValue(mapper.readTree(requestEnvelope).get("payload"), HistoryRequest.class))));
    }

    private JsonNode apiPayload(String requestEnvelope) throws Exception {
        String body = mapper.readTree(requestEnvelope).get("payload").toString();
        String response = mvc.perform(post("/internal/market/history").contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", CORRELATION_ID).content(body))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(response);
    }

    @Test
    void historyApiAnswersTheSameContentAsTheQueryAnswerWhenTruncated() throws Exception {
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), anyInt())).thenReturn(rows(4));
        String request = request("\"maxPoints\": 3");

        JsonNode answer = directAnswer(request);
        JsonNode api = apiPayload(request);

        assertThat(api).isEqualTo(answer);
        assertThat(api.get("points").size()).isEqualTo(3);
        assertThat(api.get("truncated").asBoolean()).isTrue();
        assertThat(api.get("points").get(0).get("asOf").asString()).isEqualTo("2026-10-20T15:55:31+09:00");
    }

    @Test
    void thinnedHistoryApiAnswersTheSameContentAsTheQueryAnswer() throws Exception {
        when(repository.findThinnedBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), eq(10), anyInt()))
                .thenReturn(rows(5));
        String request = request("\"maxPoints\": 60,\n    \"intervalSeconds\": 10");

        JsonNode answer = directAnswer(request);
        JsonNode api = apiPayload(request);

        assertThat(api).isEqualTo(answer);
        assertThat(api.get("points").size()).isEqualTo(5);
        assertThat(api.get("truncated").asBoolean()).isFalse();
    }

    @Test
    void theApiLogLineCarriesTheRequestCorrelationIdHeader() throws Exception {
        when(repository.findBetweenInclusive(anyString(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(), anyInt())).thenReturn(List.of());

        apiPayload(request("\"maxPoints\": 5000"));

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getMDCPropertyMap()).containsEntry(CorrelationId.KEY, CORRELATION_ID);
    }

    @Test
    void anInvalidRequestIsABadRequest() throws Exception {
        String zeroLimit = mapper.readTree(request("\"maxPoints\": 0")).get("payload").toString();

        mvc.perform(post("/internal/market/history").contentType(MediaType.APPLICATION_JSON).content(zeroLimit))
                .andExpect(status().isBadRequest());
    }

    @Test
    void aRequestMissingARequiredFieldIsABadRequestNotAServerError() throws Exception {
        ObjectNode noUntil = (ObjectNode) mapper.readTree(request("\"maxPoints\": 5000")).get("payload");
        noUntil.remove("until");

        mvc.perform(post("/internal/market/history").contentType(MediaType.APPLICATION_JSON)
                        .content(noUntil.toString()))
                .andExpect(status().isBadRequest());
    }
}

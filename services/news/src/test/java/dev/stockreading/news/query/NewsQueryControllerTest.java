package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.JacksonJsonHttpMessageConverter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.news.Topics;
import dev.stockreading.news.api.InvalidRequestHandler;
import dev.stockreading.news.etfmap.EtfStockMembers;
import dev.stockreading.news.trace.CorrelationId;
import dev.stockreading.news.trace.CorrelationIdHttpFilter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * 기사·공시의 내부 조회 API 가 같은 입력에 Kafka 응답 payload 와 같은 내용을 돌려주는지 지킨다. 저장소는 대역이고
 * 값은 모두 가짜다. Kafka 쪽은 실제 발행기가 만든 메시지 본문을 잡아 payload 만 비교한다.
 */
class NewsQueryControllerTest {

    private static final String CORRELATION_ID = "move-2026-10-20T16:00:31+09:00";
    private static final OffsetDateTime ARTICLES_SINCE = OffsetDateTime.parse("2026-10-19T16:00:31+09:00");
    private static final OffsetDateTime ARTICLES_UNTIL = OffsetDateTime.parse("2026-10-20T16:10:31+09:00");
    private static final OffsetDateTime FILINGS_SINCE = OffsetDateTime.parse("2026-10-17T16:00:31+09:00");
    private static final OffsetDateTime FILINGS_UNTIL = OffsetDateTime.parse("2026-10-20T16:00:31+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));

    private final JsonMapper mapper = JsonMapper.builder().build();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafka = mock(KafkaTemplate.class);
    private final ArticleQueryRepository articleRepository = mock(ArticleQueryRepository.class);
    private final FilingQueryRepository filingRepository = mock(FilingQueryRepository.class);
    private final EtfStockMembers members = mock(EtfStockMembers.class);
    private final Logger controllerLog = (Logger) LoggerFactory.getLogger(NewsQueryController.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private ArticlesRequestListener articlesListener;
    private FilingsRequestListener filingsListener;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        when(members.membersOf(anyString())).thenReturn(Set.of());
        articlesListener = new ArticlesRequestListener(articleRepository, new ArticlesPublisher(kafka, mapper, CLOCK),
                mapper, members);
        filingsListener = new FilingsRequestListener(filingRepository, new FilingsPublisher(kafka, mapper, CLOCK),
                mapper);
        mvc = MockMvcBuilders.standaloneSetup(new NewsQueryController(articlesListener, filingsListener))
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

    private static List<ArticleQueryRepository.StoredArticle> articles(int count) {
        // DB 드라이버처럼 UTC 로 돌려줘, 응답이 KST 로 바꿔 쓰는 것까지 같이 비교한다. 요약이 없는 기사도 섞는다.
        return IntStream.range(0, count)
                .mapToObj(i -> new ArticleQueryRepository.StoredArticle(
                        String.format("%064d", i), "제목 " + i, i == 1 ? null : "요약 " + i, "news.example.com",
                        "https://news.example.com/articles/" + i,
                        ARTICLES_UNTIL.minusMinutes(i).withOffsetSameInstant(ZoneOffset.UTC)))
                .toList();
    }

    private static List<FilingQueryRepository.StoredFiling> filings(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new FilingQueryRepository.StoredFiling(
                        String.format("0000000000-26-%06d", i), "8-K", List.of("2.02", "9.01"),
                        FILINGS_UNTIL.minusHours(i).withOffsetSameInstant(ZoneOffset.UTC),
                        "https://filings.example.com/" + i, List.of("FAKEB", "TESTA")))
                .toList();
    }

    private JsonNode kafkaPayload(String topic) {
        ArgumentCaptor<String> message = ArgumentCaptor.captor();
        verify(kafka).send(eq(topic), anyString(), message.capture());
        return mapper.readTree(message.getValue()).get("payload");
    }

    private JsonNode apiPayload(String path, String requestEnvelope) throws Exception {
        String body = mapper.readTree(requestEnvelope).get("payload").toString();
        String response = mvc.perform(post(path).contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-Id", CORRELATION_ID).content(body))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return mapper.readTree(response);
    }

    @Test
    void articlesApiAnswersTheSameContentAsTheKafkaResponseWhenTruncated() throws Exception {
        when(articleRepository.findByRange(eq("TEST01"), any(), eq(ARTICLES_SINCE), eq(ARTICLES_UNTIL), anyInt()))
                .thenReturn(articles(4));
        String request = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"maxArticles\": 300", "\"maxArticles\": 3");

        articlesListener.onRequest(request);
        JsonNode kafka = kafkaPayload(Topics.NEWS_ARTICLES_RESPONDED);
        JsonNode api = apiPayload("/internal/news/articles", request);

        assertThat(api).isEqualTo(kafka);
        assertThat(api.get("articles").size()).isEqualTo(3);
        assertThat(api.get("truncated").asBoolean()).isTrue();
        assertThat(api.get("articles").get(0).get("publishedAt").asString()).isEqualTo("2026-10-20T16:10:31+09:00");
    }

    @Test
    void researchArticlesApiAnswersTheSameContentAsTheKafkaResponse() throws Exception {
        UUID researchJobId = UUID.fromString("0b6f3c2e-9a41-4d7e-8c15-3e2a7f9d1b60");
        when(articleRepository.findByResearchJob(eq(researchJobId), eq(ARTICLES_SINCE), eq(ARTICLES_UNTIL), anyInt()))
                .thenReturn(articles(2));
        String request = ContractSchemas.sample("news.articles.requested.sample.json").replace(
                "\"maxArticles\": 300", "\"maxArticles\": 300,\n    \"researchJobId\": \"" + researchJobId + "\"");

        articlesListener.onRequest(request);
        JsonNode kafka = kafkaPayload(Topics.NEWS_ARTICLES_RESPONDED);
        JsonNode api = apiPayload("/internal/news/articles", request);

        assertThat(api).isEqualTo(kafka);
        assertThat(api.get("articles").size()).isEqualTo(2);
    }

    @Test
    void filingsApiAnswersTheSameContentAsTheKafkaResponse() throws Exception {
        when(filingRepository.findByTickers(eq(List.of("TESTA", "FAKEB")), eq(FILINGS_SINCE), eq(FILINGS_UNTIL),
                anyInt())).thenReturn(filings(3));
        String request = ContractSchemas.sample("news.filings.requested.sample.json");

        filingsListener.onRequest(request);
        JsonNode kafka = kafkaPayload(Topics.NEWS_FILINGS_RESPONDED);
        JsonNode api = apiPayload("/internal/news/filings", request);

        assertThat(api).isEqualTo(kafka);
        assertThat(api.get("filings").size()).isEqualTo(3);
        assertThat(api.get("filings").get(0).get("items").size()).isEqualTo(2);
    }

    @Test
    void theApiLogLineCarriesTheRequestCorrelationIdHeader() throws Exception {
        when(filingRepository.findByTickers(any(), any(), any(), anyInt())).thenReturn(List.of());

        apiPayload("/internal/news/filings", ContractSchemas.sample("news.filings.requested.sample.json"));

        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getMDCPropertyMap()).containsEntry(CorrelationId.KEY, CORRELATION_ID);
    }

    @Test
    void anInvalidRequestIsABadRequest() throws Exception {
        String noTickers = mapper.readTree(ContractSchemas.sample("news.filings.requested.sample.json"))
                .get("payload").toString().replace("\"tickers\":[\"TESTA\",\"FAKEB\"]", "\"tickers\":[]");
        String badResearchId = mapper.readTree(ContractSchemas.sample("news.articles.requested.sample.json"))
                .get("payload").toString().replace("\"maxArticles\":300", "\"maxArticles\":300,\"researchJobId\":\"x\"");

        mvc.perform(post("/internal/news/filings").contentType(MediaType.APPLICATION_JSON).content(noTickers))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/internal/news/articles").contentType(MediaType.APPLICATION_JSON).content(badResearchId))
                .andExpect(status().isBadRequest());
    }

    private String payloadWithout(String file, String field) throws Exception {
        ObjectNode payload = (ObjectNode) mapper.readTree(ContractSchemas.sample(file)).get("payload");
        payload.remove(field);
        return payload.toString();
    }

    @Test
    void aRequestMissingARequiredFieldIsABadRequestNotAServerError() throws Exception {
        mvc.perform(post("/internal/news/articles").contentType(MediaType.APPLICATION_JSON)
                        .content(payloadWithout("news.articles.requested.sample.json", "since")))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/internal/news/filings").contentType(MediaType.APPLICATION_JSON)
                        .content(payloadWithout("news.filings.requested.sample.json", "until")))
                .andExpect(status().isBadRequest());
    }
}

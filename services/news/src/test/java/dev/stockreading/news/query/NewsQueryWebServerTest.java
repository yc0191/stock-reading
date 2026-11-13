package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.core.KafkaTemplate;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import dev.stockreading.news.Topics;
import dev.stockreading.news.etfmap.EtfStockMembers;
import dev.stockreading.news.trace.CorrelationId;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 로 실제 웹 서버(임의 포트)를 띄워, 컨트롤러·400 처리기·X-Correlation-Id 필터가 자동 등록되는지와
 * Boot 가 만든 직렬화로 낸 HTTP 응답이 같은 입력의 Kafka 응답 payload 와 같은지 본다. DB·Kafka 자동 설정은 끄고
 * 저장소와 KafkaTemplate 은 mock 이다. 값은 모두 가짜다.
 */
@SpringBootTest(classes = NewsQueryWebServerTest.Wiring.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class NewsQueryWebServerTest {

    private static final String CORRELATION_ID = "move-2026-10-20T16:00:31+09:00";
    private static final OffsetDateTime FILINGS_SINCE = OffsetDateTime.parse("2026-10-17T16:00:31+09:00");
    private static final OffsetDateTime FILINGS_UNTIL = OffsetDateTime.parse("2026-10-20T16:00:31+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));

    @SpringBootConfiguration
    @EnableAutoConfiguration(excludeName = {
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration",
            "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration"})
    // 운영과 같은 스캔 대상 중 이 API 에 필요한 것만 올린다. 테스트 설정 클래스는 이름이 맞지 않아 스캔되지 않는다.
    @ComponentScan(basePackages = {"dev.stockreading.news.query", "dev.stockreading.news.api",
            "dev.stockreading.news.trace"}, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = ".*\\.(NewsQueryController|InvalidRequestHandler|CorrelationIdHttpFilter)"))
    static class Wiring {

        @Bean
        ArticleQueryRepository articleQueryRepository() {
            return mock(ArticleQueryRepository.class);
        }

        @Bean
        FilingQueryRepository filingQueryRepository() {
            return mock(FilingQueryRepository.class);
        }

        @SuppressWarnings("unchecked")
        @Bean
        KafkaTemplate<String, String> kafka() {
            return mock(KafkaTemplate.class);
        }

        @Bean
        ArticlesRequestListener articlesRequestListener(ArticleQueryRepository repository,
                KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
            return new ArticlesRequestListener(repository, new ArticlesPublisher(kafka, mapper, CLOCK), mapper,
                    mock(EtfStockMembers.class));
        }

        @Bean
        FilingsRequestListener filingsRequestListener(FilingQueryRepository repository,
                KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
            return new FilingsRequestListener(repository, new FilingsPublisher(kafka, mapper, CLOCK), mapper);
        }
    }

    private final JsonMapper reader = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newHttpClient();
    private final Logger controllerLog = (Logger) LoggerFactory.getLogger(NewsQueryController.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final int port;
    private final FilingQueryRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final FilingsRequestListener listener;

    @Autowired
    NewsQueryWebServerTest(@LocalServerPort int port, FilingQueryRepository repository,
            KafkaTemplate<String, String> kafka, FilingsRequestListener listener) {
        this.port = port;
        this.repository = repository;
        this.kafka = kafka;
        this.listener = listener;
    }

    @BeforeEach
    void setUp() {
        reset(repository, kafka);
        appender.start();
        controllerLog.addAppender(appender);
    }

    @AfterEach
    void detach() {
        controllerLog.detachAppender(appender);
    }

    private static String request() throws Exception {
        return ContractSchemas.sample("news.filings.requested.sample.json");
    }

    private HttpResponse<String> post(String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/internal/news/filings"))
                        .header("Content-Type", "application/json")
                        .header(CorrelationId.HTTP_HEADER, CORRELATION_ID)
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void theRealServerAnswersTheSameJsonAsTheKafkaResponseAndLogsTheCorrelationId() throws Exception {
        // DB 드라이버처럼 UTC 로 돌려줘, 응답이 KST 로 바꿔 쓰는 것까지 같이 비교한다.
        when(repository.findByTickers(eq(List.of("TESTA", "FAKEB")), eq(FILINGS_SINCE), eq(FILINGS_UNTIL), anyInt()))
                .thenReturn(IntStream.range(0, 3).mapToObj(i -> new FilingQueryRepository.StoredFiling(
                        String.format("0000000000-26-%06d", i), "8-K", List.of("2.02", "9.01"),
                        FILINGS_UNTIL.minusHours(i).withOffsetSameInstant(ZoneOffset.UTC),
                        "https://filings.example.com/" + i, List.of("FAKEB", "TESTA"))).toList());
        String request = request();

        listener.onRequest(request);
        ArgumentCaptor<String> message = ArgumentCaptor.captor();
        verify(kafka).send(eq(Topics.NEWS_FILINGS_RESPONDED), anyString(), message.capture());
        HttpResponse<String> response = post(reader.readTree(request).get("payload").toString());

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode api = reader.readTree(response.body());
        assertThat(api).isEqualTo(reader.readTree(message.getValue()).get("payload"));
        assertThat(api.get("filings").size()).isEqualTo(3);
        // 필터가 자동 등록되어 컨트롤러 로그 줄에 요청 헤더의 추적 ID 가 붙는다.
        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getMDCPropertyMap()).containsEntry(CorrelationId.KEY, CORRELATION_ID);
    }

    @Test
    void aMissingFieldIsABadRequestOnTheRealServer() throws Exception {
        String noSince = reader.readTree(request()).get("payload").toString()
                .replace("\"since\":\"2026-10-17T16:00:31+09:00\",", "");

        assertThat(post(noSince).statusCode()).isEqualTo(400);
    }

    @Test
    void aStorageFailureIsAServerErrorWithoutTheInternalDetailInTheBody() throws Exception {
        when(repository.findByTickers(any(), any(), any(), anyInt()))
                .thenThrow(new DataAccessResourceFailureException("secret-detail news.filing"));

        HttpResponse<String> response = post(reader.readTree(request()).get("payload").toString());

        assertThat(response.statusCode()).isEqualTo(500);
        assertThat(response.body()).doesNotContain("secret-detail").doesNotContain("news.filing");
    }
}

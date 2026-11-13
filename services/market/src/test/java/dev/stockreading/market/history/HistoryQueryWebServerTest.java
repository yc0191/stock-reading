package dev.stockreading.market.history;

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
import dev.stockreading.market.Topics;
import dev.stockreading.market.trace.CorrelationId;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 로 실제 웹 서버(임의 포트)를 띄워, 컨트롤러·400 처리기·X-Correlation-Id 필터가 자동 등록되는지와
 * Boot 가 만든 직렬화로 낸 HTTP 응답이 같은 입력의 Kafka 응답 payload 와 같은지 본다. DB·Kafka 자동 설정은 끄고
 * 저장소와 KafkaTemplate 은 mock 이다. 값은 모두 가짜다.
 */
@SpringBootTest(classes = HistoryQueryWebServerTest.Wiring.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class HistoryQueryWebServerTest {

    private static final String CORRELATION_ID = "move-2026-10-20T16:00:31+09:00";
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-20T15:55:31+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T16:10:31+09:00");
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:43Z"), ZoneId.of("Asia/Seoul"));

    @SpringBootConfiguration
    @EnableAutoConfiguration(excludeName = {
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration",
            "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration"})
    // 운영과 같은 스캔 대상 중 이 API 에 필요한 것만 올린다. 테스트 설정 클래스는 이름이 맞지 않아 스캔되지 않는다.
    @ComponentScan(basePackages = {"dev.stockreading.market.history", "dev.stockreading.market.api",
            "dev.stockreading.market.trace"}, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = ".*\\.(HistoryQueryController|InvalidRequestHandler|CorrelationIdHttpFilter)"))
    static class Wiring {

        @Bean
        PriceHistoryRepository priceHistoryRepository() {
            return mock(PriceHistoryRepository.class);
        }

        @SuppressWarnings("unchecked")
        @Bean
        KafkaTemplate<String, String> kafka() {
            return mock(KafkaTemplate.class);
        }

        @Bean
        HistoryRequestListener historyRequestListener(PriceHistoryRepository repository,
                KafkaTemplate<String, String> kafka, ObjectMapper mapper) {
            return new HistoryRequestListener(repository, new HistoryPublisher(kafka, mapper, CLOCK), mapper);
        }
    }

    private final JsonMapper reader = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newHttpClient();
    private final Logger controllerLog = (Logger) LoggerFactory.getLogger(HistoryQueryController.class);
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final int port;
    private final PriceHistoryRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final HistoryRequestListener listener;

    @Autowired
    HistoryQueryWebServerTest(@LocalServerPort int port, PriceHistoryRepository repository,
            KafkaTemplate<String, String> kafka, HistoryRequestListener listener) {
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
        return ContractSchemas.sample("market.history.requested.sample.json")
                .replace("\"maxPoints\": 5000", "\"maxPoints\": 3");
    }

    private HttpResponse<String> post(String body) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/internal/market/history"))
                        .header("Content-Type", "application/json")
                        .header(CorrelationId.HTTP_HEADER, CORRELATION_ID)
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void theRealServerAnswersTheSameJsonAsTheKafkaResponseAndLogsTheCorrelationId() throws Exception {
        // DB 드라이버처럼 UTC 로 돌려줘, 응답이 KST 로 바꿔 쓰는 것까지 같이 비교한다.
        when(repository.findBetweenInclusive(eq("TEST01"), eq(SINCE), eq(UNTIL), anyInt())).thenReturn(
                IntStream.range(0, 4).mapToObj(i -> new PriceHistoryRepository.StoredPoint(
                        SINCE.plusSeconds(10L * i).withOffsetSameInstant(ZoneOffset.UTC), 10_000L + i)).toList());
        String request = request();

        listener.onRequest(request);
        ArgumentCaptor<String> message = ArgumentCaptor.captor();
        verify(kafka).send(eq(Topics.MARKET_HISTORY_RESPONDED), anyString(), message.capture());
        HttpResponse<String> response = post(reader.readTree(request).get("payload").toString());

        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode api = reader.readTree(response.body());
        assertThat(api).isEqualTo(reader.readTree(message.getValue()).get("payload"));
        assertThat(api.get("points").size()).isEqualTo(3);
        // 필터가 자동 등록되어 컨트롤러 로그 줄에 요청 헤더의 추적 ID 가 붙는다.
        assertThat(appender.list).hasSize(1);
        assertThat(appender.list.get(0).getMDCPropertyMap()).containsEntry(CorrelationId.KEY, CORRELATION_ID);
    }

    @Test
    void aMissingFieldIsABadRequestOnTheRealServer() throws Exception {
        String noSince = reader.readTree(request()).get("payload").toString()
                .replace("\"since\":\"2026-10-20T15:55:31+09:00\",", "");

        assertThat(post(noSince).statusCode()).isEqualTo(400);
    }

    @Test
    void aStorageFailureIsAServerErrorWithoutTheInternalDetailInTheBody() throws Exception {
        when(repository.findBetweenInclusive(any(), any(), any(), anyInt()))
                .thenThrow(new DataAccessResourceFailureException("secret-detail market.price_snapshot"));

        HttpResponse<String> response = post(reader.readTree(request()).get("payload").toString());

        assertThat(response.statusCode()).isEqualTo(500);
        assertThat(response.body()).doesNotContain("secret-detail").doesNotContain("price_snapshot");
    }
}

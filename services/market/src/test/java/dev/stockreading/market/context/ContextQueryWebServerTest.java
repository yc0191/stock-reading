package dev.stockreading.market.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.dao.DataAccessResourceFailureException;

import dev.stockreading.market.trace.CorrelationId;
import tools.jackson.databind.json.JsonMapper;

/**
 * application.yml 로 실제 웹 서버(임의 포트)를 띄워, 배경 자료(/context)·데일리 판 자료(/daily) 조회 중 서버 안쪽이 실패하면
 * Boot 기본 오류 응답(500, 내부 내용 없음)으로 답하는지 본다. 브리핑은 500 을 다시 시도할 오류로 보므로 400 과 섞이면 안 된다.
 * 응답 내용은 ContextQueryControllerTest 가 보므로 여기서는 계산 클래스를 mock 으로 두고 DB·Kafka 자동 설정은 끈다.
 */
@SpringBootTest(classes = ContextQueryWebServerTest.Wiring.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ContextQueryWebServerTest {

    @SpringBootConfiguration
    @EnableAutoConfiguration(excludeName = {
            "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
            "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration",
            "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration"})
    // 운영과 같은 스캔 대상 중 이 API 의 입구만 올린다. 테스트 설정 클래스는 이름이 맞지 않아 스캔되지 않는다.
    @ComponentScan(basePackages = {"dev.stockreading.market.context", "dev.stockreading.market.api",
            "dev.stockreading.market.trace"}, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = ".*\\.(ContextQueryController|InvalidRequestHandler|CorrelationIdHttpFilter)"))
    static class Wiring {

        @Bean
        ContextQuery contextQuery() {
            return mock(ContextQuery.class);
        }

        @Bean
        DailyQuery dailyQuery() {
            return mock(DailyQuery.class);
        }
    }

    private final JsonMapper reader = JsonMapper.builder().build();
    private final HttpClient http = HttpClient.newHttpClient();
    private final int port;
    private final ContextQuery context;
    private final DailyQuery daily;

    @Autowired
    ContextQueryWebServerTest(@LocalServerPort int port, ContextQuery context, DailyQuery daily) {
        this.port = port;
        this.context = context;
        this.daily = daily;
    }

    @BeforeEach
    void setUp() {
        reset(context, daily);
    }

    private HttpResponse<String> post(String path, String sample) throws Exception {
        String body = reader.readTree(ContractSchemas.sample(sample)).get("payload").toString();
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/internal/market/" + path))
                        .header("Content-Type", "application/json")
                        .header(CorrelationId.HTTP_HEADER, "move-2026-10-20T16:00:31+09:00")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aContextStorageFailureIsAServerErrorWithoutTheInternalDetailInTheBody() throws Exception {
        when(context.answer(any())).thenThrow(new DataAccessResourceFailureException("secret-detail market.background"));

        HttpResponse<String> response = post("context", "market.context.requested.sample.json");

        assertThat(response.statusCode()).isEqualTo(500);
        assertThat(response.body()).doesNotContain("secret-detail").doesNotContain("market.background");
    }

    @Test
    void aDailyStorageFailureIsAServerErrorWithoutTheInternalDetailInTheBody() throws Exception {
        when(daily.answer(any())).thenThrow(new DataAccessResourceFailureException("secret-detail market.daily"));

        HttpResponse<String> response = post("daily", "market.daily.requested.sample.json");

        assertThat(response.statusCode()).isEqualTo(500);
        assertThat(response.body()).doesNotContain("secret-detail").doesNotContain("market.daily");
    }

    @Test
    void aBadRequestStaysABadRequestOnTheRealServer() throws Exception {
        // 서버 실패(500)와 입력 오류(400)가 섞이지 않는지 같은 서버에서 함께 본다. 빈 본문은 jobId 가 없어 400 이다.
        HttpResponse<String> response = http.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/internal/market/context"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{}")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
    }
}

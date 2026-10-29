package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import dev.stockreading.news.event.ArticlesPayload;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class ArticlesPublisherTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private KafkaTemplate<String, String> kafka;
    private ArticlesPublisher publisher;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        kafka = mock(KafkaTemplate.class);
        publisher = new ArticlesPublisher(kafka, mapper, CLOCK);
    }

    @Test
    void publishedResponseMatchesTheContractAndEchoesTheRequestIdentifiers() throws IOException {
        ArticlesPayload.Article article = new ArticlesPayload.Article(
                "a".repeat(64), "테스트 기사 제목", "", "2026-10-20T16:02:00+09:00",
                "news.example.com", "https://news.example.com/articles/12345");
        publisher.publish(JOB_ID, 3, "TEST01", List.of(article), false, CORRELATION_ID);

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("news.articles.responded"), eq("TEST01"), json.capture());

        assertThat(ContractSchemas.validate("news.articles.responded.v1.schema.json", json.getValue())).isEmpty();
        JsonNode event = mapper.readTree(json.getValue());
        assertThat(event.get("eventType").asString()).isEqualTo("news.articles.responded");
        assertThat(event.get("correlationId").asString()).isEqualTo(CORRELATION_ID);
        assertThat(event.get("occurredAt").asString()).isEqualTo("2026-10-20T16:15:44+09:00");
        assertThat(event.get("payload").get("jobId").asString()).isEqualTo(JOB_ID);
        assertThat(event.get("payload").get("seq").asInt()).isEqualTo(3);
        assertThat(event.get("payload").get("articles").get(0).get("articleId").asString()).isEqualTo("a".repeat(64));
    }

    @Test
    void zeroArticlesIsAValidResponse() throws IOException {
        publisher.publish(JOB_ID, 1, "TEST01", List.of(), false, CORRELATION_ID);

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(eq("news.articles.responded"), eq("TEST01"), json.capture());

        assertThat(ContractSchemas.validate("news.articles.responded.v1.schema.json", json.getValue())).isEmpty();
        assertThat(mapper.readTree(json.getValue()).get("payload").get("articles").size()).isZero();
    }
}

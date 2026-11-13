package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/** 리플레이 안전망(ADR-003/010): 과거 샘플 이벤트를 현재 소비자가 계속 읽을 수 있어야 한다. */
class NewsContractTest {

    private static final String SCHEMA = "news.collected.v1.schema.json";
    private static final String SAMPLE = "news.collected.sample.json";

    private final ObjectMapper mapper = JsonMapper.builder().build();
    private NewsArticleRepository repository;
    private NewsListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(NewsArticleRepository.class);
        listener = new NewsListener(repository, mock(ArticleResultRepository.class), mapper);
    }

    @Test
    void v1SampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate(SCHEMA, ContractSchemas.sample(SAMPLE))).isEmpty();
    }

    @Test
    void v1SampleIsReadableByCurrentConsumer() throws IOException {
        NewsEnvelope event = mapper.readValue(ContractSchemas.sample(SAMPLE), NewsEnvelope.class);

        assertThat(event.eventType()).isEqualTo("news.collected");
        assertThat(event.payload().symbols()).containsExactly("TEST01");
        assertThat(event.payload().matchedQueries()).containsExactly("테스트검색어");
        assertThat(event.payload().researchJobId()).isNull();
    }

    @Test
    void usTickerTagsMatchTheSchemaAndAreReadByTheConsumer() throws IOException {
        String withUs = ContractSchemas.sample(SAMPLE).replace("\"symbols\": [\"TEST01\"]", "\"symbols\": [\"TEST01\", \"FAKE\", \"FAKE-B\"]");

        assertThat(ContractSchemas.validate(SCHEMA, withUs)).isEmpty();
        assertThat(mapper.readValue(withUs, NewsEnvelope.class).payload().symbols())
                .containsExactly("TEST01", "FAKE", "FAKE-B");
        assertThat(ContractSchemas.validate(SCHEMA, withUs.replace("FAKE-B", "fake-b"))).isNotEmpty();
    }

    @Test
    void researchJobIdIsReadWhenPresentAndStaysOptional() throws IOException {
        String withResearch = ContractSchemas.sample(SAMPLE).replace(
                "\"symbols\": [\"TEST01\"]",
                "\"symbols\": [\"TEST01\"],\n    \"researchJobId\": \"5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"");

        assertThat(ContractSchemas.validate(SCHEMA, withResearch)).isEmpty();
        NewsEnvelope event = mapper.readValue(withResearch, NewsEnvelope.class);
        assertThat(event.payload().researchJobId()).isEqualTo("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");

        Map<?, ?> schema = mapper.readValue(ContractSchemas.schema(SCHEMA), Map.class);
        Map<?, ?> payload = (Map<?, ?>) ((Map<?, ?>) schema.get("properties")).get("payload");
        assertThat(requiredOf(payload)).doesNotContain("researchJobId");
    }

    @Test
    void v1SampleIsConvertedToAnArticle() throws IOException {
        listener.onNews(ContractSchemas.sample(SAMPLE));

        NewsArticle article = savedArticle();
        assertThat(article.eventId()).isEqualTo(UUID.fromString("7a1d9c52-3b64-4e8f-a0c1-5d2e6f7a8b90"));
        assertThat(article.url()).isEqualTo("https://news.example.com/articles/12345");
        assertThat(article.publisher()).isEqualTo("news.example.com");
        assertThat(article.publishedAt()).isEqualTo(OffsetDateTime.parse("2026-10-20T15:42:00+09:00"));
        assertThat(article.symbols()).containsExactly("TEST01");
        assertThat(article.matchedQueries()).containsExactly("테스트검색어");
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample(SAMPLE)
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate(SCHEMA, withExtraFields)).isEmpty();
        listener.onNews(withExtraFields);

        assertThat(savedArticle().symbols()).containsExactly("TEST01");
    }

    @Test
    void schemaRequiresEveryFieldTheConsumerReads() throws IOException {
        Map<?, ?> schema = mapper.readValue(ContractSchemas.schema(SCHEMA), Map.class);
        Map<?, ?> payload = (Map<?, ?>) ((Map<?, ?>) schema.get("properties")).get("payload");

        // 소비자가 값을 꺼내 쓰는 필드가 스키마에서 선택이면, 없는 이벤트가 검증을 통과해 소비자만 깨진다.
        assertThat(requiredOf(schema)).contains("eventId", "correlationId", "payload");
        assertThat(requiredOf(payload))
                .contains("title", "summary", "url", "publisher", "publishedAt", "symbols")
                .doesNotContain("matchedQueries");
    }

    private NewsArticle savedArticle() {
        ArgumentCaptor<NewsArticle> captor = ArgumentCaptor.forClass(NewsArticle.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }

    @SuppressWarnings("unchecked")
    private static List<String> requiredOf(Map<?, ?> object) {
        return (List<String>) object.get("required");
    }
}

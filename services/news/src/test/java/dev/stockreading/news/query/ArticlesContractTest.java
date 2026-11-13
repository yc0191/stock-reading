package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.stockreading.news.event.ArticlesPayload;
import dev.stockreading.news.event.EventFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * 내부 조회 API 의 요청·응답 본문은 옛 요청·응답 이벤트의 payload 계약(contracts/events/, 더 이상 쓰지 않는 토픽)과 같다.
 * 남겨 둔 샘플을 지금 요청 본문으로 계속 읽을 수 있고, 지금 응답 본문이 그 계약을 지키는지 본다(ADR-010).
 */
class ArticlesContractTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-20T07:15:44Z"), ZoneId.of("Asia/Seoul"));
    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";

    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void requestSampleMatchesSchemaAndIsReadableByTheConsumer() throws IOException {
        String sample = ContractSchemas.sample("news.articles.requested.sample.json");

        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json", sample)).isEmpty();
        ArticlesRequest request = payload(sample, ArticlesRequest.class);
        assertThat(request.jobId()).isEqualTo("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");
        assertThat(request.seq()).isEqualTo(1);
        assertThat(request.symbol()).isEqualTo("TEST01");
        assertThat(request.maxArticles()).isEqualTo(300);
        assertThat(request.researchJobId()).isNull();
    }

    @Test
    void researchJobIdIsReadWhenPresentAndMustBeAUuid() throws IOException {
        String sample = ContractSchemas.sample("news.articles.requested.sample.json");
        String withResearch = sample.replace(
                "\"maxArticles\": 300", "\"maxArticles\": 300,\n    \"researchJobId\": \"5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"");

        assertThat(withResearch).isNotEqualTo(sample);
        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json", withResearch)).isEmpty();
        assertThat(payload(withResearch, ArticlesRequest.class).researchJobId())
                .isEqualTo("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13");
        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json",
                withResearch.replace("5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13\"\n", "not-a-uuid\"\n"))).isNotEmpty();
    }

    @Test
    void responseSampleMatchesSchema() throws IOException {
        assertThat(ContractSchemas.validate("news.articles.responded.v1.schema.json",
                ContractSchemas.sample("news.articles.responded.sample.json"))).isEmpty();
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() throws IOException {
        String withExtraFields = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"payload\": {", "\"newTopLevelHint\": \"x\",\n  \"payload\": {\n    \"venue\": \"future\",");

        assertThat(ContractSchemas.validate("news.articles.requested.v1.schema.json", withExtraFields)).isEmpty();
        assertThat(payload(withExtraFields, ArticlesRequest.class).symbol())
                .isEqualTo("TEST01");
    }

    @Test
    void answerPayloadMatchesTheResponseContractWithAndWithoutArticles() throws IOException {
        ArticlesPayload.Article article = new ArticlesPayload.Article("a".repeat(64), "테스트 기사 제목", "",
                "2026-10-20T16:02:00+09:00", "news.example.com", "https://news.example.com/articles/12345");
        String one = serialize(new ArticlesPayload(JOB_ID, 3, "TEST01", List.of(article), false));
        String none = serialize(new ArticlesPayload(JOB_ID, 1, "TEST01", List.of(), false));

        assertThat(ContractSchemas.validate("news.articles.responded.v1.schema.json", one)).isEmpty();
        assertThat(ContractSchemas.validate("news.articles.responded.v1.schema.json", none)).isEmpty();
        assertThat(mapper.readTree(one).get("payload").get("articles").get(0).get("articleId").asString())
                .isEqualTo("a".repeat(64));
    }

    // 응답 본문을 옛 응답 이벤트 봉투에 담아 계약 스키마로 검사한다.
    private String serialize(ArticlesPayload payload) {
        return mapper.writeValueAsString(EventFactory.create(CLOCK, "news.articles.responded",
                OffsetDateTime.now(CLOCK), "c", payload));
    }

    /** 요청 이벤트 샘플의 payload 를 내부 조회 API 요청 본문으로 읽는다. 요청 본문은 그 payload 와 같은 모양이다. */
    private <T> T payload(String event, Class<T> type) {
        return mapper.treeToValue(mapper.readTree(event).get("payload"), type);
    }
}

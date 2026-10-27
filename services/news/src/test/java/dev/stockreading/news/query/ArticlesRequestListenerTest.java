package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import dev.stockreading.news.event.ArticlesPayload;
import tools.jackson.databind.json.JsonMapper;

class ArticlesRequestListenerTest {

    private static final String JOB_ID = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
    private static final String CORRELATION_ID = "collect-2026-10-20T16:00:30+09:00";
    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-19T16:00:31+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T16:10:31+09:00");
    private static final OffsetDateTime NEWEST = OffsetDateTime.parse("2026-10-20T16:02:00+09:00");

    private ArticleQueryRepository repository;
    private ArticlesPublisher publisher;
    private ArticlesRequestListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(ArticleQueryRepository.class);
        publisher = mock(ArticlesPublisher.class);
        listener = new ArticlesRequestListener(repository, publisher, JsonMapper.builder().build());
    }

    private static List<ArticleQueryRepository.StoredArticle> rows(int count, String summary) {
        return IntStream.range(0, count)
                .mapToObj(i -> new ArticleQueryRepository.StoredArticle(
                        String.format("%064d", i), "제목 " + i, summary, "news.example.com",
                        "https://news.example.com/articles/" + i, NEWEST.minusMinutes(i)))
                .toList();
    }

    @Test
    void respondsWithEveryRowInTheRequestedRangeNewestFirstAndEchoesTheIdentifiers() throws IOException {
        when(repository.findByRange(eq("TEST01"), eq(SINCE), eq(UNTIL), anyInt())).thenReturn(rows(3, "요약"));

        listener.onRequest(ContractSchemas.sample("news.articles.requested.sample.json"));

        ArgumentCaptor<List<ArticlesPayload.Article>> articles = ArgumentCaptor.captor();
        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), articles.capture(), eq(false), eq(CORRELATION_ID));
        assertThat(articles.getValue()).hasSize(3);
        // articleId는 저장된 article_key 그대로, 발행 시각은 RFC 3339 형식이다.
        assertThat(articles.getValue().get(0).articleId()).isEqualTo(String.format("%064d", 0));
        assertThat(articles.getValue().get(0).publishedAt()).isEqualTo("2026-10-20T16:02:00+09:00");
    }

    @Test
    void zeroRowsIsStillAnswered() throws IOException {
        when(repository.findByRange(anyString(), any(), any(), anyInt())).thenReturn(List.of());

        listener.onRequest(ContractSchemas.sample("news.articles.requested.sample.json"));

        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), eq(List.of()), eq(false), eq(CORRELATION_ID));
    }

    @Test
    void readsOneRowMoreThanTheLimitAndMarksTheResponseTruncated() throws IOException {
        String request = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"maxArticles\": 300", "\"maxArticles\": 2");
        when(repository.findByRange(eq("TEST01"), eq(SINCE), eq(UNTIL), eq(3))).thenReturn(rows(3, "요약"));

        listener.onRequest(request);

        ArgumentCaptor<List<ArticlesPayload.Article>> articles = ArgumentCaptor.captor();
        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), articles.capture(), eq(true), eq(CORRELATION_ID));
        // 최신 기사가 남는다.
        assertThat(articles.getValue()).extracting(ArticlesPayload.Article::title).containsExactly("제목 0", "제목 1");
    }

    @Test
    void exactlyTheLimitIsNotTruncated() throws IOException {
        String request = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"maxArticles\": 300", "\"maxArticles\": 3");
        when(repository.findByRange(anyString(), any(), any(), eq(4))).thenReturn(rows(3, "요약"));

        listener.onRequest(request);

        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), any(), eq(false), eq(CORRELATION_ID));
    }

    @Test
    void capsTheRequestedLimit() throws IOException {
        String request = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"maxArticles\": 300", "\"maxArticles\": 1000000");
        when(repository.findByRange(anyString(), any(), any(), anyInt())).thenReturn(List.of());

        listener.onRequest(request);

        verify(repository).findByRange("TEST01", SINCE, UNTIL, ArticlesRequestListener.MAX_ARTICLES_CAP + 1);
    }

    @Test
    void dropsOlderArticlesOnceTheByteBudgetIsExceededSoTheMessageFitsTheBrokerLimit() throws IOException {
        // 한글 2000자 요약은 UTF-8로 약 6KB다. 150건이면 예산(800KB)을 넘는다.
        String longSummary = "가".repeat(2000);
        String request = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"maxArticles\": 300", "\"maxArticles\": 500");
        when(repository.findByRange(anyString(), any(), any(), anyInt())).thenReturn(rows(150, longSummary));

        listener.onRequest(request);

        ArgumentCaptor<List<ArticlesPayload.Article>> articles = ArgumentCaptor.captor();
        verify(publisher).publish(eq(JOB_ID), eq(1), eq("TEST01"), articles.capture(), eq(true), eq(CORRELATION_ID));
        assertThat(articles.getValue()).isNotEmpty().hasSizeLessThan(150);
        long bytes = articles.getValue().stream()
                .mapToLong(a -> JsonMapper.builder().build().writeValueAsBytes(a).length + 1).sum();
        assertThat(bytes).isLessThanOrEqualTo(ArticlesRequestListener.MAX_ARTICLES_BYTES);
        // 버려지는 쪽은 오래된 기사다. 가장 최신 기사는 남는다.
        assertThat(articles.getValue().get(0).title()).isEqualTo("제목 0");
    }

    @Test
    void invalidRequestIsRejectedWithoutQueryingOrAnswering() throws IOException {
        String zeroLimit = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"maxArticles\": 300", "\"maxArticles\": 0");
        String reversedRange = ContractSchemas.sample("news.articles.requested.sample.json")
                .replace("\"since\": \"2026-10-19T16:00:31+09:00\"", "\"since\": \"2026-10-21T00:00:00+09:00\"");

        assertThatThrownBy(() -> listener.onRequest(zeroLimit)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> listener.onRequest(reversedRange)).isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(repository, publisher);
    }

    @Test
    void doesNotAnswerWhenTheQueryFails() throws IOException {
        when(repository.findByRange(anyString(), any(), any(), anyInt())).thenThrow(new IllegalStateException("db down"));

        assertThatThrownBy(() -> listener.onRequest(ContractSchemas.sample("news.articles.requested.sample.json")))
                .isInstanceOf(IllegalStateException.class);

        verify(publisher, never()).publish(anyString(), anyInt(), anyString(), any(), anyBoolean(), anyString());
    }
}

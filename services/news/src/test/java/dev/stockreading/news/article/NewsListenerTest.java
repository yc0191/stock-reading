package dev.stockreading.news.article;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import tools.jackson.databind.json.JsonMapper;

class NewsListenerTest {

    private static final UUID EVENT_ID = UUID.fromString("7a1f9e60-2b3c-4d5e-8f90-a1b2c3d4e5f6");

    private NewsArticleRepository repository;
    private ArticleResultRepository results;
    private NewsListener listener;

    @BeforeEach
    void setUp() {
        repository = mock(NewsArticleRepository.class);
        results = mock(ArticleResultRepository.class);
        listener = new NewsListener(repository, results, JsonMapper.builder().build());
    }

    @Test
    void mapsTheEventToAnArticle() {
        when(repository.save(any())).thenReturn(true);

        listener.onNews(event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", "[\"테스트검색어\"]", ""));

        assertThat(savedArticles(1).get(0)).isEqualTo(new NewsArticle(
                EVENT_ID,
                "테스트 제목",
                "",
                "https://news.example.com/article/1",
                "news.example.com",
                OffsetDateTime.parse("2026-10-20T15:30:00+09:00"),
                List.of("TEST01"),
                List.of("테스트검색어")));
    }

    // 저장 끝 알림이 결과별로 셀 수 있게, 새 기사는 '새로 저장', 이미 있던 기사(URL 중복)는 '합침'으로 남긴다.
    @Test
    void recordsWhetherTheEventStoredANewArticleOrMergedIntoAnExistingOne() {
        when(repository.save(any())).thenReturn(true, false);
        UUID otherEventId = UUID.fromString("8b2f0f71-3c4d-4e6f-9fa1-b2c3d4e5f607");

        listener.onNews(event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", null, ""));
        listener.onNews(event(otherEventId, "https://news.example.com/article/1", "[\"TEST02\"]", null, ""));

        verify(results).record(EVENT_ID, ArticleResult.STORED, "collect-2026-10-20T16:00:00+09:00");
        verify(results).record(otherEventId, ArticleResult.MERGED, "collect-2026-10-20T16:00:00+09:00");
    }

    @Test
    void failedStorageRecordsNoResultSoTheErrorHandlerRetries() {
        when(repository.save(any())).thenThrow(new IllegalStateException("db down"));

        assertThatThrownBy(() -> listener.onNews(
                event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", null, "")))
                .isInstanceOf(IllegalStateException.class);

        verifyNoInteractions(results);
    }

    @Test
    void sameEventDeliveredTwiceIsSavedWithTheSameArticleBothTimes() {
        // 멱등성은 저장소가 보장하므로 소비자는 같은 내용으로 같은 저장 호출만 해야 한다.
        when(repository.save(any())).thenReturn(true, false);
        String message = event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", "[\"테스트검색어\"]", "");

        listener.onNews(message);
        listener.onNews(message);

        List<NewsArticle> saved = savedArticles(2);
        assertThat(saved.get(0)).isEqualTo(saved.get(1));
    }

    @Test
    void sameArticleFromAnotherEventCarriesItsOwnSymbolsAndQueriesForTheUnion() {
        when(repository.save(any())).thenReturn(true, false);
        UUID otherEventId = UUID.fromString("8b2f0f71-3c4d-4e6f-9fa1-b2c3d4e5f607");

        listener.onNews(event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", "[\"검색어A\"]", ""));
        listener.onNews(event(otherEventId, "https://www.news.example.com/article/1/?utm_source=x",
                "[\"TEST02\",\"*\"]", "[\"검색어B\"]", ""));

        List<NewsArticle> saved = savedArticles(2);
        assertThat(saved.get(0).symbols()).containsExactly("TEST01");
        assertThat(saved.get(1).symbols()).containsExactly("TEST02", "*");
        assertThat(saved.get(1).matchedQueries()).containsExactly("검색어B");
        // 서로 다른 URL 표기여도 같은 기사로 모이는지는 ArticleKeyTest가 검증한다.
        assertThat(ArticleKey.of(saved.get(0).url())).isEqualTo(ArticleKey.of(saved.get(1).url()));
    }

    @Test
    void missingMatchedQueriesBecomeAnEmptyList() {
        when(repository.save(any())).thenReturn(true);

        listener.onNews(event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", null, ""));

        assertThat(savedArticles(1).get(0).matchedQueries()).isEmpty();
    }

    @Test
    void articleFromRegularCollectionHasNoResearchJobId() {
        when(repository.save(any())).thenReturn(true);

        listener.onNews(event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", "[\"테스트검색어\"]", ""));

        assertThat(savedArticles(1).get(0).researchJobId()).isNull();
    }

    @Test
    void researchJobIdIsPassedToStorageSoTheArticleIsMarkedAsFoundByThatResearch() {
        when(repository.save(any())).thenReturn(false);
        String researchJobId = "5c2e8a41-7d3f-4b6a-a1c9-0e4f2b8d6a13";
        String message = event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", "[\"테스트 재검색어\"]", "")
                .replace("\"symbols\":", "\"researchJobId\":\"" + researchJobId + "\",\"symbols\":");

        listener.onNews(message);

        NewsArticle saved = savedArticles(1).get(0);
        assertThat(saved.researchJobId()).isEqualTo(UUID.fromString(researchJobId));
        assertThat(saved.symbols()).containsExactly("TEST01");
    }

    @Test
    void consumerIgnoresFieldsAddedInFutureVersions() {
        when(repository.save(any())).thenReturn(true);
        String message = event(EVENT_ID, "https://news.example.com/article/1", "[\"TEST01\"]", null, "")
                .replace("\"payload\":", "\"futureEnvelopeField\":1,\"payload\":")
                .replace("\"title\":", "\"futurePayloadField\":\"x\",\"title\":");

        listener.onNews(message);

        verify(repository).save(any());
    }

    private List<NewsArticle> savedArticles(int expectedCalls) {
        ArgumentCaptor<NewsArticle> captor = ArgumentCaptor.forClass(NewsArticle.class);
        verify(repository, times(expectedCalls)).save(captor.capture());
        return captor.getAllValues();
    }

    private static String event(UUID eventId, String url, String symbolsJson, String matchedQueriesJson, String summary) {
        String queries = matchedQueriesJson == null ? "" : ",\"matchedQueries\":" + matchedQueriesJson;
        return """
                {"eventId":"%s","eventType":"news.collected","schemaVersion":1,
                 "occurredAt":"2026-10-20T15:30:00+09:00","producedAt":"2026-10-20T16:00:00+09:00",
                 "correlationId":"collect-2026-10-20T16:00:00+09:00",
                 "payload":{"title":"테스트 제목","summary":"%s","url":"%s","publisher":"news.example.com",
                            "publishedAt":"2026-10-20T15:30:00+09:00","symbols":%s%s}}
                """.formatted(eventId, summary, url, symbolsJson, queries);
    }
}

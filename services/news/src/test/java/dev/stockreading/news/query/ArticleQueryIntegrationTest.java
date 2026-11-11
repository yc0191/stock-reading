package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import dev.stockreading.news.event.ArticlesPayload;
import dev.stockreading.news.etfmap.EtfStockMembers;
import tools.jackson.databind.json.JsonMapper;

/**
 * 기사 조회를 실제 Postgres에 저장된 기사로 확인한다. 세 환경변수가 모두 있을 때만 실행하며,
 * 대상은 news 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 평소 gradlew test 에서는 건너뛴다.
 */
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
class ArticleQueryIntegrationTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-03-11T09:00:00+09:00");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-03-12T09:00:00+09:00");

    private static JdbcClient jdbc;
    private static ArticleQueryRepository repository;

    @BeforeAll
    static void migrateAndSeed() {
        String url = System.getenv("NEWS_IT_DB_URL");
        String user = System.getenv("NEWS_IT_DB_USER");
        String password = System.getenv("NEWS_IT_DB_PASSWORD");

        // 서비스 설정(application.yml)과 같이 news 스키마에만 마이그레이션하고, 스키마는 만들지 않는다.
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("news").defaultSchema("news").createSchemas(false).load().migrate();

        jdbc = JdbcClient.create(new DriverManagerDataSource(url + (url.contains("?") ? "&" : "?") + "currentSchema=news",
                user, password));
        repository = new ArticleQueryRepository(jdbc);

        // 이 패키지는 쓰기 경로가 없으므로 기사와 종목 태그를 SQL로 직접 넣는다.
        insert("q-before", SINCE.minusSeconds(1), "ITQ001");
        insert("q-since", SINCE, "ITQ001");
        insert("q-market", SINCE.plusHours(1), "*");
        insert("q-both", SINCE.plusHours(2), "ITQ001", "*");
        insert("q-other", SINCE.plusHours(3), "ITQ002");
        insert("q-until", UNTIL, "ITQ001");
        insert("q-after", UNTIL.plusSeconds(1), "ITQ001");
        // 구성 종목 기사. 국내 코드, 미국 티커(종류 구분 "-" 포함)를 모두 태그 칸에 넣을 수 있어야 한다.
        insert("m-old-us", SINCE.plusMinutes(10), "ITM-US", "ITMEM1");
        insert("m-new-kr", UNTIL.minusMinutes(10), "ITMEM1");
        insert("m-new-us-b", UNTIL.minusMinutes(5), "ITUS-B");
        insert("m-with-etf", UNTIL.minusMinutes(1), "ITMEM1", "ITQ003");
        insert("m-own", SINCE.plusMinutes(5), "ITQ003");

        // 재검색 기사는 요청 종목(ITQ001)과 "*" 가 아닌 태그를 붙인다. 작업 id 로 읽을 때 종목 조건을 보지 않는지 확인하고,
        // 종목으로 읽는 기존 테스트의 결과에는 섞이지 않게 한다.
        insertResearch("r-before", SINCE.minusSeconds(1), RESEARCH_JOB, "ITR001");
        insertResearch("r-since", SINCE, RESEARCH_JOB, "ITR001");
        insertResearch("r-middle", SINCE.plusHours(4), RESEARCH_JOB, "ITR003");
        insertResearch("r-other-job", SINCE.plusHours(5), OTHER_RESEARCH_JOB, "ITR001");
        insertResearch("r-until", UNTIL, RESEARCH_JOB, "ITR002");
        insertResearch("r-after", UNTIL.plusSeconds(1), RESEARCH_JOB, "ITR001");
    }

    private static final UUID RESEARCH_JOB = UUID.fromString("b1000000-0000-4000-8000-000000000001");
    private static final UUID OTHER_RESEARCH_JOB = UUID.fromString("b2000000-0000-4000-8000-000000000002");

    @Test
    void researchJobQueryReturnsOnlyThatJobsArticlesInRangeWhateverTheirTags() {
        List<ArticleQueryRepository.StoredArticle> found = repository.findByResearchJob(RESEARCH_JOB, SINCE, UNTIL, 100);

        // 기간 양끝은 포함하고, 다른 작업의 기사와 정기 수집 기사(q-*)는 빠진다.
        assertThat(found).extracting(ArticleQueryRepository.StoredArticle::title)
                .containsExactly("r-until", "r-middle", "r-since");
    }

    @Test
    void researchJobQueryKeepsTheNewestWithinTheLimit() {
        assertThat(repository.findByResearchJob(RESEARCH_JOB, SINCE, UNTIL, 1))
                .extracting(ArticleQueryRepository.StoredArticle::title).containsExactly("r-until");
    }

    @Test
    void requestWithAResearchJobIdIsAnsweredWithThatJobsArticlesUnderTheRequestSymbol() {
        ArticlesPublisher publisher = mock(ArticlesPublisher.class);
        ArticlesRequestListener listener = new ArticlesRequestListener(repository, publisher, JsonMapper.builder().build(), mock(EtfStockMembers.class));

        listener.onRequest(request(100).replace("\"maxArticles\":100", "\"maxArticles\":100,\"researchJobId\":\""
                + RESEARCH_JOB + "\""));

        ArgumentCaptor<List<ArticlesPayload.Article>> articles = ArgumentCaptor.captor();
        verify(publisher).publish(eq("it-job"), eq(1), eq("ITQ001"), articles.capture(), eq(false), eq("it"));
        assertThat(articles.getValue()).extracting(ArticlesPayload.Article::title)
                .containsExactly("r-until", "r-middle", "r-since");
    }

    @Test
    void returnsTheSymbolAndMarketWideArticlesInRangeNewestFirst() {
        List<ArticleQueryRepository.StoredArticle> found = repository.findByRange("ITQ001", Set.of(), SINCE, UNTIL, 100);

        // 기간 양끝은 포함하고, 두 태그를 모두 가진 기사는 한 번만, 다른 종목의 기사는 빠진다.
        assertThat(found).extracting(ArticleQueryRepository.StoredArticle::title)
                .containsExactly("q-until", "q-both", "q-market", "q-since");
        assertThat(found.get(0).publishedAt().toInstant()).isEqualTo(UNTIL.toInstant());
        assertThat(found.get(3).publishedAt().toInstant()).isEqualTo(SINCE.toInstant());
    }

    @Test
    void etfArticlesComeFirstThenMemberArticlesWithoutDuplicatesEachNewestFirst() {
        List<ArticleQueryRepository.StoredArticle> found = repository.findByRange("ITQ003",
                Set.of("ITMEM1", "ITUS-B"), SINCE, UNTIL, 100);

        // ETF 자체(ITQ003, "*") 기사가 최신순으로 먼저, 구성 종목 기사가 그다음 최신순이다. m-with-etf 는 두 쪽에 걸려도 앞에만 나온다.
        assertThat(found).extracting(ArticleQueryRepository.StoredArticle::title).containsExactly(
                "m-with-etf", "q-both", "q-market", "m-own",
                "m-new-us-b", "m-new-kr", "m-old-us");
    }

    @Test
    void etfArticlesSurviveTheLimitEvenWhenMemberArticlesAreNewerAndMoreNumerous() {
        List<ArticleQueryRepository.StoredArticle> found = repository.findByRange("ITQ003",
                Set.of("ITMEM1", "ITUS-B"), SINCE, UNTIL, 4);

        assertThat(found).extracting(ArticleQueryRepository.StoredArticle::title)
                .containsExactly("m-with-etf", "q-both", "q-market", "m-own");
    }

    @Test
    void noMembersReadsOnlyTheSymbolsOwnArticles() {
        assertThat(repository.findByRange("ITQ003", Set.of(), SINCE, UNTIL, 100))
                .extracting(ArticleQueryRepository.StoredArticle::title)
                .containsExactly("m-with-etf", "q-both", "q-market", "m-own");
    }

    @Test
    void limitKeepsTheNewestArticles() {
        assertThat(repository.findByRange("ITQ001", Set.of(), SINCE, UNTIL, 2))
                .extracting(ArticleQueryRepository.StoredArticle::title).containsExactly("q-until", "q-both");
    }

    @Test
    void responseIsCutAtMaxArticlesAndMarkedTruncated() {
        ArticlesPublisher publisher = mock(ArticlesPublisher.class);
        ArticlesRequestListener listener = new ArticlesRequestListener(repository, publisher, JsonMapper.builder().build(), mock(EtfStockMembers.class));

        listener.onRequest(request(3));

        ArgumentCaptor<List<ArticlesPayload.Article>> articles = ArgumentCaptor.captor();
        verify(publisher).publish(eq("it-job"), eq(1), eq("ITQ001"), articles.capture(), eq(true), eq("it"));
        assertThat(articles.getValue()).extracting(ArticlesPayload.Article::title)
                .containsExactly("q-until", "q-both", "q-market");
    }

    private static void insert(String title, OffsetDateTime publishedAt, String... symbols) {
        String url = "https://news.example.com/it/" + title;
        long id = jdbc.sql("""
                        INSERT INTO news_article (event_id, article_key, title, summary, url, publisher, published_at)
                        VALUES (gen_random_uuid(), encode(sha256(convert_to(:url, 'UTF8')), 'hex'), :title, '요약', :url,
                                'news.example.com', :publishedAt)
                        RETURNING id
                        """)
                .param("url", url).param("title", title).param("publishedAt", publishedAt).query(Long.class).single();
        for (String symbol : symbols) {
            jdbc.sql("INSERT INTO news_article_symbol (article_id, symbol) VALUES (:id, :symbol)")
                    .param("id", id).param("symbol", symbol).update();
        }
    }

    private static void insertResearch(String title, OffsetDateTime publishedAt, UUID jobId, String symbol) {
        insert(title, publishedAt, symbol);
        jdbc.sql("UPDATE news_article SET from_research = true, research_job_id = :jobId WHERE title = :title")
                .param("jobId", jobId).param("title", title).update();
    }

    private static String request(int maxArticles) {
        return """
                {"eventId":"e1000000-0000-4000-8000-000000000001","eventType":"news.articles.requested","schemaVersion":1,
                 "occurredAt":"2026-03-12T09:10:00+09:00","producedAt":"2026-03-12T09:10:00+09:00","correlationId":"it",
                 "payload":{"jobId":"it-job","seq":1,"symbol":"ITQ001",
                            "since":"2026-03-11T09:00:00+09:00","until":"2026-03-12T09:00:00+09:00","maxArticles":%d}}
                """.formatted(maxArticles);
    }
}

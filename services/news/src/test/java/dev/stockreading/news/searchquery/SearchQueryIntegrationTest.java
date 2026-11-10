package dev.stockreading.news.searchquery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import dev.stockreading.news.event.SearchQueriesPayload.Query;

/**
 * 검색어 테이블의 마이그레이션, 처음 채우기, 전체 목록 읽기를 실제 Postgres로 확인한다. 세 환경변수가 모두 있을 때만
 * 실행하며, 대상은 news 스키마가 비어 있는 테스트 전용 DB라고 가정한다. 이 테이블만 쓰므로 다른 테스트 클래스와
 * 데이터가 겹치지 않는다.
 */
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
class SearchQueryIntegrationTest {

    private static JdbcClient jdbc;
    private static SearchQueryRepository repository;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("NEWS_IT_DB_URL");
        String user = System.getenv("NEWS_IT_DB_USER");
        String password = System.getenv("NEWS_IT_DB_PASSWORD");
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration")
                .schemas("news").defaultSchema("news").createSchemas(false).load().migrate();
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                url + (url.contains("?") ? "&" : "?") + "currentSchema=news", user, password);
        jdbc = JdbcClient.create(dataSource);
        repository = new SearchQueryRepository(jdbc, new TransactionTemplate(new DataSourceTransactionManager(dataSource)));
    }

    @BeforeEach
    void startEmpty() {
        jdbc.sql("DELETE FROM search_query").update();
    }

    @Test
    void isEmptyReflectsWhetherTheTableHasRows() {
        assertThat(repository.isEmpty()).isTrue();

        repository.insertAll(List.of(new Query("통합 검색어 하나", List.of("TEST01"))));

        assertThat(repository.isEmpty()).isFalse();
    }

    @Test
    void insertedQueriesAreReadBackAsOneListWithDomesticUsAndMarketWideTags() {
        repository.insertAll(SearchQuerySpec.parse("통합 검색어 하나=TEST01,TEST02;통합 미국=FAKE.B;통합 시황=*"));

        SearchQuerySnapshot snapshot = repository.readSnapshot();

        assertThat(snapshot.queries()).extracting(Query::query).containsExactlyInAnyOrder("통합 검색어 하나", "통합 미국", "통합 시황");
        assertThat(snapshot.queries()).filteredOn(q -> q.query().equals("통합 검색어 하나"))
                .singleElement().satisfies(q -> assertThat(q.symbols()).containsExactly("TEST01", "TEST02"));
        assertThat(snapshot.queries()).filteredOn(q -> q.query().equals("통합 미국"))
                .singleElement().satisfies(q -> assertThat(q.symbols()).containsExactly("FAKE-B"));
        assertThat(snapshot.queries()).filteredOn(q -> q.query().equals("통합 시황"))
                .singleElement().satisfies(q -> assertThat(q.symbols()).containsExactly("*"));
    }

    @Test
    void editingTheTablesDirectlyChangesTheHashAndAQueryWithNoTagIsLeftOut() {
        repository.insertAll(SearchQuerySpec.parse("통합 검색어 하나=TEST01"));
        String before = repository.readSnapshot().contentHash();

        jdbc.sql("INSERT INTO search_query_symbol (query_id, symbol) SELECT id, 'TEST02' FROM search_query").update();
        String afterTag = repository.readSnapshot().contentHash();
        jdbc.sql("INSERT INTO search_query (query) VALUES ('통합 태그 없는 검색어')").update();
        String afterTaglessQuery = repository.readSnapshot().contentHash();

        assertThat(afterTag).isNotEqualTo(before);
        // 태그가 없는 검색어는 기사에 붙일 종목이 없어 목록에서 빠지므로 해시가 그대로다.
        assertThat(afterTaglessQuery).isEqualTo(afterTag);
        assertThat(repository.readSnapshot().queries()).hasSize(1);
    }

    @Test
    void failedFillLeavesNothingBehind() {
        // 같은 검색어가 두 번 들어가 두 번째에서 실패하면 첫 검색어도 남지 않는다.
        List<Query> duplicate = List.of(new Query("통합 중복", List.of("TEST01")), new Query("통합 중복", List.of("TEST02")));

        assertThatThrownBy(() -> repository.insertAll(duplicate)).isInstanceOf(RuntimeException.class);

        assertThat(repository.isEmpty()).isTrue();
    }

    @Test
    void tableRulesRejectBadRows() {
        repository.insertAll(SearchQuerySpec.parse("통합 검색어 하나=TEST01"));

        assertThatThrownBy(() -> jdbc.sql("INSERT INTO search_query (query) VALUES ('   ')").update())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql("INSERT INTO search_query (query) VALUES ('통합 검색어 하나')").update())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> jdbc.sql(
                "INSERT INTO search_query_symbol (query_id, symbol) SELECT id, 'test01' FROM search_query").update())
                .isInstanceOf(RuntimeException.class);
        // 6자 칸에 들어가지 않는 미국 티커(종류 구분 포함)도 이 테이블에는 들어간다.
        jdbc.sql("INSERT INTO search_query_symbol (query_id, symbol) SELECT id, 'ABCDEF-XY' FROM search_query").update();
    }

    @Test
    void deletingAQueryDeletesItsTags() {
        repository.insertAll(SearchQuerySpec.parse("통합 검색어 하나=TEST01,TEST02"));

        jdbc.sql("DELETE FROM search_query").update();

        assertThat(jdbc.sql("SELECT count(*) FROM search_query_symbol").query(Long.class).single()).isZero();
    }
}

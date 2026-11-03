package dev.stockreading.news.query;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 실제 Postgres 대상 통합 테스트. 세 환경변수가 모두 있을 때만 실행하며, 대상은 비어 있는 새 DB(public 스키마)라고 가정한다.
 * 평소 gradlew test 에서는 건너뛴다. 테스트마다 다른 가짜 티커를 써서 서로의 행을 읽지 않게 한다.
 */
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
class FilingsQueryIntegrationTest {

    private static final OffsetDateTime SINCE = OffsetDateTime.parse("2026-10-17T07:00:31Z");
    private static final OffsetDateTime UNTIL = OffsetDateTime.parse("2026-10-20T07:00:31Z");

    private static JdbcClient jdbc;
    private static FilingQueryRepository repository;

    @BeforeAll
    static void migrateAndWire() {
        String url = System.getenv("NEWS_IT_DB_URL");
        String user = System.getenv("NEWS_IT_DB_USER");
        String password = System.getenv("NEWS_IT_DB_PASSWORD");
        Flyway.configure().dataSource(url, user, password).locations("classpath:db/migration").load().migrate();
        jdbc = JdbcClient.create(new DriverManagerDataSource(url, user, password));
        repository = new FilingQueryRepository(jdbc);
    }

    @Test
    void filingsAcceptedExactlyAtBothEndsAreIncludedAndOneSecondOutsideAreNot() {
        insert("0000000051-26-000001", SINCE.minusSeconds(1), "QTESTA");
        insert("0000000051-26-000002", SINCE, "QTESTA");
        insert("0000000051-26-000003", UNTIL, "QTESTA");
        insert("0000000051-26-000004", UNTIL.plusSeconds(1), "QTESTA");

        List<FilingQueryRepository.StoredFiling> rows = repository.findByTickers(List.of("QTESTA"), SINCE, UNTIL, 10);

        assertThat(rows).extracting(FilingQueryRepository.StoredFiling::accessionNumber)
                .containsExactly("0000000051-26-000003", "0000000051-26-000002");
    }

    @Test
    void newestFirstUpToTheLimitAndAFilingOfTwoRequestedTickersAppearsOnce() {
        insert("0000000052-26-000001", UNTIL.minusHours(3), "QTESTB");
        insert("0000000052-26-000002", UNTIL.minusHours(2), "QTESTB", "QTESTC");
        insert("0000000052-26-000003", UNTIL.minusHours(1), "QTESTC");

        List<FilingQueryRepository.StoredFiling> rows =
                repository.findByTickers(List.of("QTESTB", "QTESTC"), SINCE, UNTIL, 2);

        assertThat(rows).extracting(FilingQueryRepository.StoredFiling::accessionNumber)
                .containsExactly("0000000052-26-000003", "0000000052-26-000002");
        assertThat(rows.get(1).tickers()).containsExactly("QTESTB", "QTESTC");
    }

    @Test
    void filingsOfOtherTickersAreNotReadAndOtherLinkedTickersAreNotReturned() {
        insert("0000000053-26-000001", UNTIL.minusHours(1), "QTESTD", "QOTHER");
        insert("0000000053-26-000002", UNTIL.minusHours(1), "QOTHR2");

        List<FilingQueryRepository.StoredFiling> rows = repository.findByTickers(List.of("QTESTD"), SINCE, UNTIL, 10);

        assertThat(rows).singleElement().satisfies(row -> {
            assertThat(row.accessionNumber()).isEqualTo("0000000053-26-000001");
            assertThat(row.tickers()).containsExactly("QTESTD");
            assertThat(row.items()).containsExactly("2.02");
            assertThat(row.acceptedAt()).isEqualTo(UNTIL.minusHours(1));
        });
    }

    private static void insert(String accessionNumber, OffsetDateTime acceptedAt, String... tickers) {
        long id = jdbc.sql("""
                        INSERT INTO filing (event_id, accession_number, cik, form, items, accepted_at, filing_date, url)
                        VALUES (:eventId, :accession, '0000000021', '8-K', CAST(:items AS text[]), :acceptedAt,
                                CAST(:acceptedAt AS date), 'https://www.example.com/filings/1')
                        RETURNING id
                        """)
                .param("eventId", UUID.randomUUID())
                .param("accession", accessionNumber)
                .param("items", new String[] {"2.02"})
                .param("acceptedAt", acceptedAt)
                .query(Long.class).single();
        for (String ticker : tickers) {
            jdbc.sql("INSERT INTO filing_ticker (filing_id, ticker) VALUES (:id, :ticker)")
                    .param("id", id).param("ticker", ticker).update();
        }
    }
}

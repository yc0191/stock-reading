package dev.stockreading.news.query;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** filing·filing_ticker를 읽기만 한다. 응답을 만들기 위한 조회라 쓰기 경로는 없다. */
@Repository
class FilingQueryRepository {

    private final JdbcClient jdbc;

    FilingQueryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * tickers 중 하나라도 연결된 공시를 접수 시각 [since, until] 구간(양끝 포함)에서 최신순으로 최대 limit건 돌려준다.
     * 한 공시에 여러 티커가 연결돼도 한 건으로 나오도록 조인 대신 EXISTS를 쓴다. 공시마다 돌려주는 티커는
     * 요청한 티커 중 연결된 것뿐이다(다른 ETF의 보유 종목이 이 종목의 근거에 섞이지 않게).
     * 접수 시각이 같으면 접수번호로 순서를 고정해 같은 조건의 응답이 늘 같은 순서가 되게 한다.
     */
    List<StoredFiling> findByTickers(List<String> tickers, OffsetDateTime since, OffsetDateTime until, int limit) {
        String[] tickerArray = tickers.toArray(String[]::new);
        return jdbc.sql("""
                        SELECT f.accession_number, f.form, f.items, f.accepted_at, f.url,
                               ARRAY(SELECT t.ticker FROM filing_ticker t
                                     WHERE t.filing_id = f.id AND t.ticker = ANY(CAST(:tickers AS varchar[]))
                                     ORDER BY t.ticker) AS tickers
                        FROM filing f
                        WHERE f.accepted_at >= :since AND f.accepted_at <= :until
                          AND EXISTS (SELECT 1 FROM filing_ticker t
                                      WHERE t.filing_id = f.id AND t.ticker = ANY(CAST(:tickers AS varchar[])))
                        ORDER BY f.accepted_at DESC, f.accession_number DESC
                        LIMIT :limit
                        """)
                .param("tickers", tickerArray)
                .param("since", since)
                .param("until", until)
                .param("limit", limit)
                .query((rs, rowNum) -> new StoredFiling(
                        rs.getString("accession_number"),
                        rs.getString("form"),
                        Arrays.asList((String[]) rs.getArray("items").getArray()),
                        rs.getObject("accepted_at", OffsetDateTime.class),
                        rs.getString("url"),
                        Arrays.asList((String[]) rs.getArray("tickers").getArray())))
                .list();
    }

    record StoredFiling(
            String accessionNumber,
            String form,
            List<String> items,
            OffsetDateTime acceptedAt,
            String url,
            List<String> tickers) {
    }
}

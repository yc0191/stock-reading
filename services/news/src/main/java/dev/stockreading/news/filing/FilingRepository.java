package dev.stockreading.news.filing;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
class FilingRepository {

    private final JdbcClient jdbc;

    FilingRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 멱등 저장: 같은 이벤트(eventId)를 몇 번 받아도, 같은 공시(접수번호)가 다른 이벤트로 와도 공시는 한 행이다.
     * 기존 공시면 티커만 합집합으로 늘린다. 한 트랜잭션이라 중간 상태가 남지 않는다.
     *
     * @return 새 행이 만들어졌으면 true
     */
    @Transactional
    boolean save(Filing filing) {
        Optional<Long> inserted = insertIfAbsent(filing);
        long filingId = inserted.orElseGet(() -> findIdByAccession(filing.accessionNumber()));

        jdbc.sql("""
                        INSERT INTO filing_ticker (filing_id, ticker)
                        SELECT :filingId, t FROM unnest(CAST(:tickers AS varchar[])) AS t
                        ON CONFLICT DO NOTHING
                        """)
                .param("filingId", filingId)
                .param("tickers", filing.tickers().toArray(String[]::new))
                .update();
        return inserted.isPresent();
    }

    /**
     * 티커가 연결된 공시 중 접수 시각이 [from, to] 안인 것을 접수 시각 순으로 읽는다.
     * 기준 시각 필터는 FilingQuery가 정해 넘긴다.
     */
    List<StoredFiling> findByTicker(String ticker, OffsetDateTime from, OffsetDateTime to) {
        return jdbc.sql("""
                        SELECT f.accession_number, f.cik, f.form, f.items, f.accepted_at, f.filing_date, f.url,
                               ARRAY(SELECT t.ticker FROM filing_ticker t WHERE t.filing_id = f.id ORDER BY t.ticker) AS tickers
                        FROM filing f
                        WHERE f.accepted_at >= :from AND f.accepted_at <= :to
                          AND EXISTS (SELECT 1 FROM filing_ticker t WHERE t.filing_id = f.id AND t.ticker = :ticker)
                        ORDER BY f.accepted_at, f.accession_number
                        """)
                .param("ticker", ticker)
                .param("from", from)
                .param("to", to)
                .query((rs, rowNum) -> new StoredFiling(
                        rs.getString("accession_number"),
                        rs.getString("cik"),
                        rs.getString("form"),
                        Arrays.asList((String[]) rs.getArray("items").getArray()),
                        rs.getObject("accepted_at", OffsetDateTime.class),
                        rs.getObject("filing_date", LocalDate.class),
                        rs.getString("url"),
                        Arrays.asList((String[]) rs.getArray("tickers").getArray())))
                .list();
    }

    // 유니크 제약이 event_id와 accession_number 둘이라 충돌 대상을 지정하지 않는다.
    private Optional<Long> insertIfAbsent(Filing filing) {
        return jdbc.sql("""
                        INSERT INTO filing
                            (event_id, accession_number, cik, form, items, accepted_at, filing_date, url)
                        VALUES
                            (:eventId, :accessionNumber, :cik, :form, CAST(:items AS text[]),
                             :acceptedAt, :filingDate, :url)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """)
                .param("eventId", filing.eventId())
                .param("accessionNumber", filing.accessionNumber())
                .param("cik", filing.cik())
                .param("form", filing.form())
                .param("items", filing.items().toArray(String[]::new))
                .param("acceptedAt", filing.acceptedAt())
                .param("filingDate", filing.filingDate())
                .param("url", filing.url())
                .query(Long.class)
                .optional();
    }

    private long findIdByAccession(String accessionNumber) {
        return jdbc.sql("SELECT id FROM filing WHERE accession_number = :accessionNumber")
                .param("accessionNumber", accessionNumber)
                .query(Long.class)
                .optional()
                .orElseThrow(() -> new IllegalStateException(
                        "filing conflicted on event_id but not accession_number, accession=" + accessionNumber));
    }
}

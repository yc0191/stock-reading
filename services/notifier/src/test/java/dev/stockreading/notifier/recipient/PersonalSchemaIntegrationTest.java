package dev.stockreading.notifier.recipient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * personal 스키마의 계정별 권한과 표 규칙, 수집 대상 뷰(collect_target)를 실제 Postgres로 확인한다. 핵심은 개인정보인 member_mail을
 * 발송기 계정과 쓰기 계정만 읽을 수 있다는 것이다(시세처리기와 수집기 계정은 읽지 못한다).
 */
@EnabledIfEnvironmentVariable(named = "NOTIFIER_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NOTIFIER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NOTIFIER_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NEWS_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class PersonalSchemaIntegrationTest {

    private final JdbcClient writer = PersonalDb.writer();
    private final JdbcClient notifier = PersonalDb.notifier();
    private final JdbcClient collector = PersonalDb.collector();
    private final JdbcClient news = PersonalDb.news();
    private final JdbcClient market = PersonalDb.market();

    @BeforeEach
    void startWithOneMember() {
        PersonalDb.reset();
        market.sql("INSERT INTO etf (market, symbol, name) VALUES ('KR', 'NOTI01', 'NOTI ETF'), ('US', 'NOTI02', NULL)")
                .update();
        market.sql("INSERT INTO stock (market, symbol) VALUES ('US', 'NOTIA'), ('US', 'NOTIB'), ('KR', 'NOTIC'), "
                + "('US', 'NOTID')").update();
        market.sql("INSERT INTO etf_stock_map (etf_id, stock_id, rank) SELECT e.id, s.id, 1 FROM etf e, stock s "
                + "WHERE e.symbol = 'NOTI01' AND s.symbol = 'NOTIA'").update();
        market.sql("INSERT INTO etf_stock_map (etf_id, stock_id, rank) SELECT e.id, s.id, 2 FROM etf e, stock s "
                + "WHERE e.symbol = 'NOTI01' AND s.symbol = 'NOTIC'").update();
        long memberId = writer.sql("INSERT INTO personal.member (name) VALUES ('NOTI MEMBER') RETURNING id")
                .query(Long.class).single();
        writer.sql("INSERT INTO personal.member_mail (member_id, email) VALUES (:m, 'schema@example.com')")
                .param("m", memberId).update();
    }

    private static void assertDenied(Runnable statement) {
        // 스프링이 감싼 예외라 가장 깊은 원인(드라이버가 낸 Postgres 메시지)에서 거부를 확인한다.
        assertThatThrownBy(statement::run).rootCause().hasMessageContaining("permission denied");
    }

    @Test
    void onlyTheNotifierAndTheWriterCanReadMemberMail() {
        assertThat(notifier.sql("SELECT email FROM personal.member_mail").query(String.class).list())
                .containsExactly("schema@example.com");
        assertThat(writer.sql("SELECT email FROM personal.member_mail").query(String.class).list())
                .containsExactly("schema@example.com");

        assertDenied(() -> market.sql("SELECT email FROM personal.member_mail").query(String.class).list());
        assertDenied(() -> collector.sql("SELECT email FROM personal.member_mail").query(String.class).list());
    }

    @Test
    void theNotifierOnlyReadsPersonalDataAndNeverWritesIt() {
        assertDenied(() -> notifier.sql("UPDATE personal.member_mail SET enabled = FALSE").update());
        assertDenied(() -> notifier.sql("DELETE FROM personal.member_portfolio").update());
        assertDenied(() -> notifier.sql("INSERT INTO personal.member (name) VALUES ('NOTI X')").update());
        assertThat(notifier.sql("SELECT count(*) FROM market.etf WHERE symbol = 'NOTI01'").query(Long.class).single())
                .isEqualTo(1L);
    }

    @Test
    void theMarketAccountReadsOnlyTheActivePortfolioViewAndTheCollectorOnlyItsTwoViews() {
        assertThat(market.sql("SELECT count(*) FROM personal.active_portfolio").query(Long.class).single()).isZero();
        assertDenied(() -> market.sql("SELECT count(*) FROM personal.member_portfolio").query(Long.class).single());
        assertDenied(() -> market.sql("SELECT count(*) FROM personal.member").query(Long.class).single());
        assertDenied(() -> market.sql("INSERT INTO personal.member_portfolio (member_id, relation) VALUES (1, 'HELD')")
                .update());

        assertThat(collector.sql("SELECT count(*) FROM personal.collect_target").query(Long.class).single()).isZero();
        assertThat(collector.sql("SELECT count(*) FROM market.news_search_target").query(Long.class).single())
                .isNotNull();
        assertDenied(() -> collector.sql("SELECT count(*) FROM personal.member_portfolio").query(Long.class).single());
        assertDenied(() -> collector.sql("SELECT count(*) FROM personal.member").query(Long.class).single());
        assertDenied(() -> collector.sql("SELECT count(*) FROM market.price_snapshot").query(Long.class).single());
    }

    // 수집기는 미국 티커를 담은 ETF를 찾으려고, 뉴스는 ETF 기사를 구성 종목 기사로 넓히려고 구성 종목 표를 읽는다. 읽기만 주고 쓰기와 개인정보는 막는다.
    @Test
    void theCollectorAndNewsAccountsReadTheConstituentTablesButCannotWriteThemOrReadPersonalData() {
        for (JdbcClient reader : List.of(collector, news)) {
            assertThat(reader.sql("SELECT count(*) FROM market.etf WHERE symbol = 'NOTI01'").query(Long.class).single())
                    .isEqualTo(1L);
            assertThat(reader.sql("SELECT count(*) FROM market.stock WHERE symbol LIKE 'NOTI%'").query(Long.class).single())
                    .isEqualTo(4L);
            assertThat(reader.sql("SELECT count(*) FROM market.etf_stock_map m JOIN market.etf e ON e.id = m.etf_id "
                    + "WHERE e.symbol = 'NOTI01'").query(Long.class).single()).isEqualTo(2L);

            assertDenied(() -> reader.sql("INSERT INTO market.stock (market, symbol) VALUES ('US', 'NOTIX')").update());
            assertDenied(() -> reader.sql("UPDATE market.etf SET name = 'x'").update());
            assertDenied(() -> reader.sql("DELETE FROM market.etf_stock_map").update());
            assertDenied(() -> reader.sql("SELECT email FROM personal.member_mail").query(String.class).list());
            assertDenied(() -> reader.sql("SELECT count(*) FROM personal.member_portfolio").query(Long.class).single());
        }
        // 뉴스 계정은 수집 대상 뷰도 읽지 못한다(개인 포트폴리오에서 나온 값이라 수집기 몫이다).
        assertDenied(() -> news.sql("SELECT count(*) FROM personal.collect_target").query(Long.class).single());
    }

    @Test
    void collectTargetIsTheEnabledMembersPortfolioWithDomesticQuotesAndUsDailyFromEtfConstituents() {
        long memberId = writer.sql("SELECT id FROM personal.member WHERE name = 'NOTI MEMBER'").query(Long.class).single();
        // 국내 ETF(담은 구성 종목 중 미국 NOTIA, 국내 NOTIC), 미국 ETF(1초 시세 대상이 아님), 미국 주식 NOTIB, 국내 주식 NOTIC
        portfolioEtf(memberId, "NOTI01");
        portfolioEtf(memberId, "NOTI02");
        portfolioStock(memberId, "NOTIB");
        portfolioStock(memberId, "NOTIC");
        // 사용 중이 아닌 멤버의 포트폴리오는 들어가지 않는다.
        long off = writer.sql("INSERT INTO personal.member (name, enabled) VALUES ('NOTI OFF', FALSE) RETURNING id")
                .query(Long.class).single();
        portfolioStock(off, "NOTID");

        List<String> rows = collector.sql("SELECT purpose || ':' || market || ':' || symbol || ':' || kind "
                + "FROM personal.collect_target ORDER BY 1").query(String.class).list();

        assertThat(rows).containsExactly("KR_QUOTE:KR:NOTI01:ETF", "KR_QUOTE:KR:NOTIC:STOCK",
                "US_DAILY:US:NOTIA:STOCK", "US_DAILY:US:NOTIB:STOCK");
    }

    @Test
    void portfolioAndMailRulesRejectBadRows() {
        long memberId = writer.sql("SELECT id FROM personal.member WHERE name = 'NOTI MEMBER'").query(Long.class).single();
        portfolioEtf(memberId, "NOTI01");

        // 한 줄은 ETF와 주식 중 정확히 하나를 가리켜야 하고, 같은 종목을 두 번 담을 수 없다.
        assertThatThrownBy(() -> writer.sql("INSERT INTO personal.member_portfolio (member_id, etf_id, stock_id, relation) "
                + "SELECT :m, e.id, s.id, 'HELD' FROM market.etf e, market.stock s "
                + "WHERE e.symbol = 'NOTI01' AND s.symbol = 'NOTIA'").param("m", memberId).update())
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> writer.sql("INSERT INTO personal.member_portfolio (member_id, relation) VALUES (:m, 'HELD')")
                .param("m", memberId).update()).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> portfolioEtf(memberId, "NOTI01")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> writer.sql("INSERT INTO personal.member_portfolio (member_id, stock_id, relation) "
                + "SELECT :m, id, 'OWNED' FROM market.stock WHERE symbol = 'NOTIA'").param("m", memberId).update())
                .isInstanceOf(RuntimeException.class);
        // 멤버 하나에 메일 줄은 하나이고, 주소는 겹칠 수 없다.
        assertThatThrownBy(() -> writer.sql("INSERT INTO personal.member_mail (member_id, email) VALUES (:m, 'another@example.com')")
                .param("m", memberId).update()).isInstanceOf(RuntimeException.class);
        long second = writer.sql("INSERT INTO personal.member (name) VALUES ('NOTI SECOND') RETURNING id")
                .query(Long.class).single();
        assertThatThrownBy(() -> writer.sql("INSERT INTO personal.member_mail (member_id, email) VALUES (:m, 'schema@example.com')")
                .param("m", second).update()).isInstanceOf(RuntimeException.class);
    }

    @Test
    void aMarketRowInAPortfolioCannotBeDeleted() {
        long memberId = writer.sql("SELECT id FROM personal.member WHERE name = 'NOTI MEMBER'").query(Long.class).single();
        portfolioStock(memberId, "NOTIA");

        assertThatThrownBy(() -> market.sql("DELETE FROM stock WHERE symbol = 'NOTIA'").update())
                .isInstanceOf(RuntimeException.class);
    }

    private void portfolioEtf(long memberId, String symbol) {
        writer.sql("INSERT INTO personal.member_portfolio (member_id, etf_id, relation) "
                        + "SELECT :m, id, 'HELD' FROM market.etf WHERE symbol = :s")
                .param("m", memberId).param("s", symbol).update();
    }

    private void portfolioStock(long memberId, String symbol) {
        writer.sql("INSERT INTO personal.member_portfolio (member_id, stock_id, relation) "
                        + "SELECT :m, id, 'WATCH' FROM market.stock WHERE symbol = :s")
                .param("m", memberId).param("s", symbol).update();
    }
}

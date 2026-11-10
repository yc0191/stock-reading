package dev.stockreading.notifier.recipient;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * 받는 사람 고르기를 실제 Postgres로 확인한다(읽기 전용 발송기 계정으로 읽는다). 급변동은 그 종목을 포트폴리오에 가진 멤버만,
 * 데일리 판은 그 판 수신을 켠 멤버, 멤버와 메일 줄이 모두 사용 중이어야 한다.
 */
@EnabledIfEnvironmentVariable(named = "NOTIFIER_IT_DB_URL", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NOTIFIER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "NOTIFIER_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "WRITER_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "COLLECTOR_IT_DB_PASSWORD", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_USER", matches = ".+")
@EnabledIfEnvironmentVariable(named = "MARKET_IT_DB_PASSWORD", matches = ".+")
class RecipientIntegrationTest {

    private final JdbcClient writer = PersonalDb.writer();
    private JdbcRecipientRepository repository;

    @BeforeEach
    void startWithEtfAndStock() {
        PersonalDb.reset();
        PersonalDb.market().sql("INSERT INTO etf (market, symbol) VALUES ('KR', 'NOTI01'), ('KR', 'NOTI02')").update();
        PersonalDb.market().sql("INSERT INTO stock (market, symbol) VALUES ('US', 'NOTIA')").update();
        repository = new JdbcRecipientRepository(PersonalDb.notifier());
    }

    private long member(String name, boolean enabled) {
        return writer.sql("INSERT INTO personal.member (name, enabled) VALUES (:n, :e) RETURNING id")
                .param("n", name).param("e", enabled).query(Long.class).single();
    }

    private long mail(long memberId, String address, boolean move, boolean morning, boolean postClose, boolean enabled) {
        return writer.sql("""
                        INSERT INTO personal.member_mail (member_id, email, receive_move, receive_morning,
                                                          receive_post_close, enabled)
                        VALUES (:m, :a, :mv, :mo, :pc, :en) RETURNING id
                        """)
                .param("m", memberId).param("a", address).param("mv", move).param("mo", morning)
                .param("pc", postClose).param("en", enabled).query(Long.class).single();
    }

    private void holdEtf(long memberId, String symbol) {
        writer.sql("INSERT INTO personal.member_portfolio (member_id, etf_id, relation) "
                        + "VALUES (:m, (SELECT id FROM market.etf WHERE symbol = :s), 'HELD')")
                .param("m", memberId).param("s", symbol).update();
    }

    private void watchStock(long memberId, String symbol) {
        writer.sql("INSERT INTO personal.member_portfolio (member_id, stock_id, relation) "
                        + "VALUES (:m, (SELECT id FROM market.stock WHERE symbol = :s), 'WATCH')")
                .param("m", memberId).param("s", symbol).update();
    }

    private static List<String> addresses(List<Recipient> recipients) {
        return recipients.stream().map(Recipient::address).toList();
    }

    @Test
    void aMoveGoesOnlyToEnabledMembersWhoReceiveMovesAndHoldThatSymbolInThePortfolio() {
        long holder = member("NOTI HOLDER", true);
        mail(holder, "holder@example.com", true, true, true, true);
        holdEtf(holder, "NOTI01");
        long stockWatcher = member("NOTI STOCK WATCHER", true);
        mail(stockWatcher, "stock@example.com", true, true, true, true);
        watchStock(stockWatcher, "NOTIA");
        long otherEtf = member("NOTI OTHER ETF", true);
        mail(otherEtf, "other@example.com", true, true, true, true);
        holdEtf(otherEtf, "NOTI02");
        long noMoves = member("NOTI NO MOVES", true);
        mail(noMoves, "nomoves@example.com", false, true, true, true);
        holdEtf(noMoves, "NOTI01");
        long mailOff = member("NOTI MAIL OFF", true);
        mail(mailOff, "mailoff@example.com", true, true, true, false);
        holdEtf(mailOff, "NOTI01");
        long memberOff = member("NOTI MEMBER OFF", false);
        mail(memberOff, "memberoff@example.com", true, true, true, true);
        holdEtf(memberOff, "NOTI01");
        member("NOTI NO PORTFOLIO", true);

        assertThat(addresses(repository.forMove("NOTI01"))).containsExactly("holder@example.com");
        assertThat(addresses(repository.forMove("NOTIA"))).containsExactly("stock@example.com");
        assertThat(addresses(repository.forMove("NOTI02"))).containsExactly("other@example.com");
        assertThat(repository.forMove("NOTI99")).isEmpty();
        assertThat(repository.forMove(null)).isEmpty();
        assertThat(repository.forMove(" ")).isEmpty();
    }

    @Test
    void aMemberHoldingTheSameSymbolInTwoPortfolioRowsIsChosenOnce() {
        long holder = member("NOTI TWICE", true);
        long mailId = mail(holder, "twice@example.com", true, true, true, true);
        holdEtf(holder, "NOTI01");
        // 같은 멤버가 ETF도 주식도 담아도 한 번만 고른다.
        watchStock(holder, "NOTIA");

        assertThat(repository.forMove("NOTI01")).containsExactly(new Recipient(mailId, "twice@example.com"));
    }

    @Test
    void anEditionGoesToEnabledMembersWhoReceiveThatEditionInMailOrder() {
        long first = member("NOTI FIRST", true);
        mail(first, "first@example.com", true, true, false, true);
        long second = member("NOTI SECOND", true);
        mail(second, "second@example.com", true, false, true, true);
        long both = member("NOTI BOTH", true);
        mail(both, "both@example.com", true, true, true, true);
        long off = member("NOTI OFF", false);
        mail(off, "off@example.com", true, true, true, true);
        long mailOff = member("NOTI MAIL OFF", true);
        mail(mailOff, "mailoff@example.com", true, true, true, false);

        assertThat(addresses(repository.forEdition("morning"))).containsExactly("first@example.com", "both@example.com");
        assertThat(addresses(repository.forEdition("post-close"))).containsExactly("second@example.com", "both@example.com");
        assertThat(repository.forEdition("midnight")).isEmpty();
        assertThat(repository.forEdition(null)).isEmpty();
    }

    @Test
    void nobodyChosenGivesAnEmptyListNotAnError() {
        assertThat(repository.forEdition("morning")).isEmpty();
        assertThat(repository.forMove("NOTI01")).isEmpty();
    }
}

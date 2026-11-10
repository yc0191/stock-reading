package dev.stockreading.notifier.recipient;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * personal 스키마(member, member_mail, member_portfolio)와 market.etf·market.stock을 읽기 전용 계정으로 읽는다.
 * 멤버와 메일 줄이 모두 사용 중(enabled)일 때만 받는 사람이 된다.
 */
@Repository
class JdbcRecipientRepository implements RecipientPort {

    private static final String BASE = """
            SELECT mail.id, mail.email FROM personal.member_mail mail
            JOIN personal.member member ON member.id = mail.member_id
            WHERE member.enabled AND mail.enabled
            """;

    private final JdbcClient jdbc;

    JdbcRecipientRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public List<Recipient> forMove(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return List.of();
        }
        return jdbc.sql(BASE + """
                        AND mail.receive_move
                        AND EXISTS (
                            SELECT 1 FROM personal.member_portfolio portfolio
                            LEFT JOIN market.etf etf ON etf.id = portfolio.etf_id
                            LEFT JOIN market.stock stock ON stock.id = portfolio.stock_id
                            WHERE portfolio.member_id = member.id AND (etf.symbol = :symbol OR stock.symbol = :symbol))
                        ORDER BY mail.id
                        """)
                .param("symbol", symbol).query(JdbcRecipientRepository::row).list();
    }

    @Override
    public List<Recipient> forEdition(String edition) {
        // 열 이름은 고정된 두 값에서만 고르므로 SQL에 이어 붙여도 값이 섞이지 않는다.
        String column = switch (edition == null ? "" : edition) {
            case "morning" -> "receive_morning";
            case "post-close" -> "receive_post_close";
            default -> null;
        };
        if (column == null) {
            return List.of();
        }
        return jdbc.sql(BASE + "AND mail." + column + " ORDER BY mail.id").query(JdbcRecipientRepository::row).list();
    }

    private static Recipient row(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
        return new Recipient(rs.getLong("id"), rs.getString("email"));
    }
}

package dev.stockreading.notifier.recipient;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 실제 DB 테스트가 계정별로 접속하는 도우미. 계정마다 환경변수가 따로 있고, 모두 있을 때만 테스트가 켜진다.
 * personal 스키마와 market 표는 전체 실행 스크립트(또는 compose의 마이그레이션)가 먼저 만들어 둔다.
 * 시작할 때 personal 표를 모두 비우고(쓰기 계정) NOTI 접두어 market 행을 지우므로 다른 테스트 클래스의 데이터와 겹치지 않게 NOTI 접두어만 쓴다.
 */
final class PersonalDb {

    private PersonalDb() {
    }

    static JdbcClient notifier() {
        return client(System.getenv("NOTIFIER_IT_DB_USER"), System.getenv("NOTIFIER_IT_DB_PASSWORD"));
    }

    static JdbcClient writer() {
        return client(System.getenv("WRITER_IT_DB_USER"), System.getenv("WRITER_IT_DB_PASSWORD"));
    }

    static JdbcClient collector() {
        return client(System.getenv("COLLECTOR_IT_DB_USER"), System.getenv("COLLECTOR_IT_DB_PASSWORD"));
    }

    static JdbcClient market() {
        return client(System.getenv("MARKET_IT_DB_USER"), System.getenv("MARKET_IT_DB_PASSWORD"));
    }

    private static JdbcClient client(String user, String password) {
        return JdbcClient.create(new DriverManagerDataSource(System.getenv("NOTIFIER_IT_DB_URL"), user, password));
    }

    /** 이 접두어 테스트가 만든 행을 모두 지운다. 포트폴리오가 market 행을 가리키므로 personal 쪽을 먼저 비운다. */
    static void reset() {
        DisposableDatabase.require(writer());
        writer().sql("DELETE FROM personal.member_portfolio").update();
        writer().sql("DELETE FROM personal.member").update();
        JdbcClient market = market();
        market.sql("DELETE FROM etf_stock_map WHERE etf_id IN (SELECT id FROM etf WHERE symbol LIKE 'NOTI%')").update();
        market.sql("DELETE FROM etf WHERE symbol LIKE 'NOTI%'").update();
        market.sql("DELETE FROM stock WHERE symbol LIKE 'NOTI%'").update();
    }
}

package dev.stockreading.collector;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * 실제 DB 테스트가 계정별로 접속하는 도우미. 계정마다 환경변수가 따로 있고, 모두 있을 때만 테스트가 켜진다.
 * market·personal 표는 전체 실행 스크립트(또는 compose의 마이그레이션)가 먼저 만들어 둔다.
 * 테스트 데이터는 COLL 접두어만 쓰고, 시작할 때 personal 표를 모두 비운다(쓰기 계정).
 */
public final class CollectorDb {

    private static final String PREFIX = "stock_test_run_";

    private CollectorDb() {
    }

    public static JdbcClient collector() {
        return client(System.getenv("COLLECTOR_IT_DB_USER"), System.getenv("COLLECTOR_IT_DB_PASSWORD"));
    }

    public static JdbcClient writer() {
        return client(System.getenv("WRITER_IT_DB_USER"), System.getenv("WRITER_IT_DB_PASSWORD"));
    }

    public static JdbcClient market() {
        return client(System.getenv("MARKET_IT_DB_USER"), System.getenv("MARKET_IT_DB_PASSWORD"));
    }

    private static JdbcClient client(String user, String password) {
        return JdbcClient.create(new DriverManagerDataSource(System.getenv("COLLECTOR_IT_DB_URL"), user, password));
    }

    /** 이 접두어 테스트가 만든 행을 모두 지운다. 포트폴리오가 market 행을 가리키므로 personal 쪽을 먼저 비운다. */
    public static void reset() {
        String database = writer().sql("SELECT current_database()").query(String.class).single();
        if (!database.startsWith(PREFIX)) {
            throw new IllegalStateException("실제 DB 테스트는 " + PREFIX + "로 시작하는 일회용 데이터베이스에서만 돌 수 있습니다: " + database);
        }
        writer().sql("DELETE FROM personal.member_portfolio").update();
        writer().sql("DELETE FROM personal.member").update();
        JdbcClient market = market();
        market.sql("DELETE FROM news_keyword WHERE keyword LIKE 'COLL%'").update();
        market.sql("DELETE FROM macro_keyword WHERE name LIKE 'COLL%'").update();
        market.sql("DELETE FROM etf_stock_map WHERE etf_id IN (SELECT id FROM etf WHERE symbol LIKE 'COLL%')").update();
        market.sql("DELETE FROM etf WHERE symbol LIKE 'COLL%'").update();
        market.sql("DELETE FROM stock WHERE symbol LIKE 'COLL%'").update();
    }
}

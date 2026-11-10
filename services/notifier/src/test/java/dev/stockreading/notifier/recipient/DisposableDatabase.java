package dev.stockreading.notifier.recipient;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * 표를 통째로 지우는 실제 DB 테스트가 개발용 데이터(보존용 DB 포함)를 지우지 않게 막는다.
 * 전체 실행 스크립트가 만드는 일회용 데이터베이스(stock_test_run_...)가 아니면 테스트를 실패시킨다.
 */
final class DisposableDatabase {

    private static final String PREFIX = "stock_test_run_";

    private DisposableDatabase() {
    }

    static void require(JdbcClient jdbc) {
        String database = jdbc.sql("SELECT current_database()").query(String.class).single();
        if (!database.startsWith(PREFIX)) {
            throw new IllegalStateException("실제 DB 테스트는 " + PREFIX + "로 시작하는 일회용 데이터베이스에서만 돌 수 있습니다: " + database);
        }
    }
}

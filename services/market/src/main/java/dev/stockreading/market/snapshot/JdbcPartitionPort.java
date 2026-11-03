package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Component
class JdbcPartitionPort implements PartitionPort {

    private static final Pattern NAME = Pattern.compile("price_snapshot_p\\d{8}");
    private static final String LOCK_KEY = "price_snapshot_partition";

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;

    JdbcPartitionPort(JdbcClient jdbc, TransactionTemplate tx) {
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @Override
    public void createIfAbsent(String name, OffsetDateTime from, OffsetDateTime to) {
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("unexpected partition name: " + name);
        }
        tx.executeWithoutResult(status -> {
            // 같은 날짜를 동시에 만들려는 쪽이 있으면 먼저 온 쪽이 끝날 때까지 기다린 뒤 이미 있음을 보고 건너뛴다.
            jdbc.sql("SELECT pg_advisory_xact_lock(hashtext(:key))").param("key", LOCK_KEY).query().singleRow();
            boolean exists = jdbc.sql("SELECT to_regclass(:name) IS NOT NULL")
                    .param("name", name).query(Boolean.class).single();
            if (exists) {
                return;
            }
            // CREATE TABLE ... PARTITION OF는 부모에 ACCESS EXCLUSIVE 잠금을 걸어 삽입과 조회를 막는다.
            // 별도 테이블을 만든 뒤 ATTACH하면 부모에는 SHARE UPDATE EXCLUSIVE만 걸려 삽입이 계속된다.
            // 이름은 정규식으로, 경계는 OffsetDateTime에서 만든 값이라 DDL에 직접 넣어도 주입 여지가 없다.
            jdbc.sql("CREATE TABLE " + name
                    + " (LIKE price_snapshot INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)").update();
            jdbc.sql("ALTER TABLE price_snapshot ATTACH PARTITION " + name
                    + " FOR VALUES FROM ('" + literal(from) + "') TO ('" + literal(to) + "')").update();
        });
    }

    private static String literal(OffsetDateTime value) {
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(value);
    }
}

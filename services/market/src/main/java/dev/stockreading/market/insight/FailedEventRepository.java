package dev.stockreading.market.insight;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
class FailedEventRepository {

    private final JdbcClient jdbc;

    FailedEventRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 멱등 삽입: 같은 위치의 레코드가 다시 복구 대상이 되어도 한 행만 남긴다. */
    void save(FailedEvent event) {
        jdbc.sql("""
                        INSERT INTO failed_event
                            (topic, partition_no, record_offset, record_key, payload,
                             exception_class, exception_message, attempts)
                        VALUES
                            (:topic, :partition, :offset, :key, :payload,
                             :exceptionClass, :exceptionMessage, :attempts)
                        ON CONFLICT (topic, partition_no, record_offset) DO NOTHING
                        """)
                .param("topic", event.topic())
                .param("partition", event.partition())
                .param("offset", event.offset())
                .param("key", event.key())
                .param("payload", event.payload())
                .param("exceptionClass", event.exceptionClass())
                .param("exceptionMessage", event.exceptionMessage())
                .param("attempts", event.attempts())
                .update();
    }
}

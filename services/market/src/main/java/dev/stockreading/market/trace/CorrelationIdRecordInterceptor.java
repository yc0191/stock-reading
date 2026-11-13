package dev.stockreading.market.trace;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * 메시지 하나를 처리하는 동안 그 메시지의 correlationId 를 로그 문맥(MDC)에 둔다. 로그 패턴의 %X{correlationId} 가 이 값을 찍는다.
 * 헤더를 먼저 보고, 헤더가 없는 메시지(헤더를 싣기 전에 쌓인 메시지)는 본문에서 읽는다.
 */
public class CorrelationIdRecordInterceptor<K, V> implements RecordInterceptor<K, V> {

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        String id = CorrelationId.fromHeaders(record.headers());
        if (id == null && record.value() instanceof String body) {
            id = CorrelationId.fromEventJson(body);
        }
        // 앞 메시지의 값이 남아 다른 메시지 로그에 찍히지 않도록, 값이 없으면 지운다.
        if (id == null) {
            MDC.remove(CorrelationId.KEY);
        } else {
            MDC.put(CorrelationId.KEY, id);
        }
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        MDC.remove(CorrelationId.KEY);
    }
}

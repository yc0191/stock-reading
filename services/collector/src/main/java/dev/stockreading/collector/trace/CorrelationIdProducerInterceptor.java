package dev.stockreading.collector.trace;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 보내는 모든 메시지의 헤더에 본문 correlationId 를 싣는다. 전송 지점마다 붙이면 새 전송에서 빠뜨리므로
 * 프로듀서 설정(spring.kafka.producer.properties.interceptor.classes)으로 한 곳에 건다. Kafka 가 직접 만들므로 public 이다.
 */
public class CorrelationIdProducerInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        if (CorrelationId.fromHeaders(record.headers()) == null) {
            String id = CorrelationId.fromEventJson(record.value());
            if (id != null) {
                record.headers().add(CorrelationId.KEY, id.getBytes(StandardCharsets.UTF_8));
            }
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}

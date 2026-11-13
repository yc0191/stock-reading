package dev.stockreading.market.trace;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mock.env.MockEnvironment;

/** 보내는 모든 메시지 헤더에 본문 correlationId 가 같은 값으로 실리는지 지킨다. */
class CorrelationIdProducerInterceptorTest {

    private final CorrelationIdProducerInterceptor interceptor = new CorrelationIdProducerInterceptor();

    private static String header(ProducerRecord<String, String> record) {
        Header header = record.headers().lastHeader(CorrelationId.KEY);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    @Test
    void theHeaderCarriesTheSameValueAsTheBody() {
        ProducerRecord<String, String> sent = interceptor.onSend(new ProducerRecord<>("topic", "key",
                "{\"eventId\":\"e-1\",\"correlationId\":\"move-005930-2026-11-13\",\"payload\":{\"correlationId\":\"inner\"}}"));

        assertThat(header(sent)).isEqualTo("move-005930-2026-11-13");
        assertThat(sent.headers().headers(CorrelationId.KEY)).hasSize(1);
    }

    @Test
    void anExistingHeaderIsNotAddedAgain() {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "{\"correlationId\":\"body\"}");
        record.headers().add(CorrelationId.KEY, "given".getBytes(StandardCharsets.UTF_8));

        assertThat(header(interceptor.onSend(record))).isEqualTo("given");
        assertThat(record.headers().headers(CorrelationId.KEY)).hasSize(1);
    }

    @Test
    void aBodyWithoutCorrelationIdIsSentWithoutTheHeader() {
        assertThat(header(interceptor.onSend(new ProducerRecord<>("topic", "key", "{\"eventId\":\"e-1\"}")))).isNull();
        assertThat(header(interceptor.onSend(new ProducerRecord<>("topic", "key", "not json")))).isNull();
        assertThat(header(interceptor.onSend(new ProducerRecord<>("topic", "key", null)))).isNull();
    }

    @Test
    void theProducerConfigurationRegistersTheInterceptor() throws IOException, ClassNotFoundException {
        // 설정에서 빠지면 전송 지점의 코드는 그대로인데 헤더만 조용히 사라진다.
        MockEnvironment environment = new MockEnvironment();
        for (PropertySource<?> source : new YamlPropertySourceLoader()
                .load("application", new FileSystemResource("src/main/resources/application.yml"))) {
            environment.getPropertySources().addLast(source);
        }
        String className = environment.getProperty("spring.kafka.producer.properties.interceptor.classes");

        assertThat(className).isEqualTo(CorrelationIdProducerInterceptor.class.getName());
        // Kafka 가 설정의 이름으로 직접 만들 수 있어야 한다.
        assertThat(Utils.newInstance(className, ProducerInterceptor.class)).isInstanceOf(CorrelationIdProducerInterceptor.class);
    }
}

package dev.stockreading.market.trace;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.logging.LoggingSystemProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.util.ReflectionTestUtils;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

/** 메시지를 처리하는 동안 찍히는 로그 줄에 그 메시지의 correlationId 가 붙는지 지킨다. */
class CorrelationIdLoggingTest {

    private static final String BODY = "{\"eventId\":\"e-1\",\"correlationId\":\"collect-2026-11-13T10:00:00+09:00\",\"payload\":{}}";

    private final CorrelationIdRecordInterceptor<String, String> interceptor = new CorrelationIdRecordInterceptor<>();
    private final Logger logger = (Logger) LoggerFactory.getLogger("correlation-id-logging-test");
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    @BeforeEach
    void attach() {
        appender.start();
        logger.addAppender(appender);
    }

    @AfterEach
    void detach() {
        logger.detachAppender(appender);
        MDC.clear();
    }

    private static ConsumerRecord<String, String> record(String body, String header) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", body);
        if (header != null) {
            record.headers().add(CorrelationId.KEY, header.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }

    @Test
    void logLinesWhileHandlingAMessageCarryItsHeaderCorrelationIdAndItIsClearedAfter() {
        ConsumerRecord<String, String> record = record(BODY, "from-header");

        interceptor.intercept(record, null);
        logger.info("handling");
        interceptor.afterRecord(record, null);
        logger.info("after");

        assertThat(appender.list.get(0).getMDCPropertyMap()).containsEntry(CorrelationId.KEY, "from-header");
        assertThat(appender.list.get(1).getMDCPropertyMap()).doesNotContainKey(CorrelationId.KEY);
    }

    @Test
    void aMessageWithoutTheHeaderUsesTheBodyCorrelationId() {
        interceptor.intercept(record(BODY, null), null);

        assertThat(MDC.get(CorrelationId.KEY)).isEqualTo("collect-2026-11-13T10:00:00+09:00");
    }

    @Test
    void aMessageWithoutAnyCorrelationIdDoesNotKeepThePreviousOne() {
        // 앞 메시지의 값이 남으면 엉뚱한 메일의 로그로 검색된다.
        MDC.put(CorrelationId.KEY, "previous");

        interceptor.intercept(record("not json", null), null);

        assertThat(MDC.get(CorrelationId.KEY)).isNull();
    }

    @Test
    void theConfiguredLogPatternPrintsTheCorrelationIdOnTheLine() throws IOException {
        MockEnvironment environment = new MockEnvironment();
        for (PropertySource<?> source : new YamlPropertySourceLoader()
                .load("application", new FileSystemResource("src/main/resources/application.yml"))) {
            environment.getPropertySources().addLast(source);
        }
        // 스프링 부트가 logging.pattern.correlation 을 기본 로그 패턴의 자리(LOG_CORRELATION_PATTERN)로 넘기는지까지 본다.
        Map<String, String> systemProperties = new HashMap<>();
        new LoggingSystemProperties(environment, systemProperties::put).apply();
        String pattern = systemProperties.get("LOG_CORRELATION_PATTERN");
        assertThat(pattern).contains("%X{correlationId");

        PatternLayout layout = new PatternLayout();
        layout.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        layout.setPattern(pattern + "%m");
        layout.start();
        interceptor.intercept(record(BODY, "abc-123"), null);
        logger.info("handling");

        assertThat(layout.doLayout(appender.list.get(0))).isEqualTo("[abc-123] handling");
    }

    @Test
    void theDefaultListenerFactoryUsesTheInterceptor() {
        // 빈으로만 두고 기본 팩토리가 가져가지 않으면 리스너 로그에 값이 조용히 빠진다.
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
                .withUserConfiguration(CorrelationIdConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    var factory = context.getBean("kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
                    assertThat(ReflectionTestUtils.getField(factory, "recordInterceptor"))
                            .isInstanceOf(CorrelationIdRecordInterceptor.class);
                });
    }
}

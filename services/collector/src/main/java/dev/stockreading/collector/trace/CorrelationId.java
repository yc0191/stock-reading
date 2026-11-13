package dev.stockreading.collector.trace;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * 이벤트 본문의 correlationId 를 Kafka 헤더, 로그 문맥(MDC), 서비스 간 HTTP 호출로 옮길 때 쓰는 이름과 읽는 법.
 * 이름의 원본은 docs/local-development.md 의 "추적 ID" 절이며, 5개 서비스가 같은 값을 쓴다(서비스끼리 코드를 나누지 않아 복제).
 */
public final class CorrelationId {

    /** Kafka 헤더 키이자 로그 문맥 키. 이벤트 본문 칸 이름과 같게 두어 로그를 같은 말로 찾게 한다. */
    public static final String KEY = "correlationId";

    /** 서비스 간 HTTP 호출에 싣는 추적 ID 헤더 이름. 값은 이벤트 본문의 correlationId 와 같다. */
    public static final String HTTP_HEADER = "X-Correlation-Id";

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private CorrelationId() {
    }

    /** 이벤트 본문(JSON) 맨 위의 correlationId. JSON 이 아니거나 값이 비었으면 null. */
    public static String fromEventJson(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode value = JSON.readTree(json).get(KEY);
            return value != null && value.isString() && !value.asString().isBlank() ? value.asString() : null;
        } catch (JacksonException e) {
            return null;
        }
    }

    /** 헤더의 correlationId. 없거나 비었으면 null. */
    public static String fromHeaders(Headers headers) {
        Header header = headers.lastHeader(KEY);
        if (header == null || header.value() == null) {
            return null;
        }
        String value = new String(header.value(), StandardCharsets.UTF_8);
        return value.isBlank() ? null : value;
    }

    /**
     * body 가 도는 동안 로그 문맥에 id 를 두고, 끝나면 앞 값으로 되돌린다. 타이머처럼 메시지 처리 밖에서 찍는 줄에 쓴다.
     * id 가 null 이면 앞 값도 지워 다른 실행의 값이 찍히지 않게 한다.
     */
    public static void within(String id, Runnable body) {
        String previous = MDC.get(KEY);
        put(id);
        try {
            body.run();
        } finally {
            put(previous);
        }
    }

    /** 부른 스레드의 로그 문맥을 다른 스레드에서 도는 task 에 옮긴다. MDC 는 스레드마다 따로라 그대로 넘기면 비어 있다. */
    public static <T> Callable<T> carry(Callable<T> task) {
        Map<String, String> context = MDC.getCopyOfContextMap();
        return () -> {
            if (context == null) {
                MDC.clear();
            } else {
                MDC.setContextMap(context);
            }
            try {
                return task.call();
            } finally {
                MDC.clear();
            }
        };
    }

    private static void put(String id) {
        if (id == null) {
            MDC.remove(KEY);
        } else {
            MDC.put(KEY, id);
        }
    }
}

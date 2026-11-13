package dev.stockreading.news.trace;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

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
}

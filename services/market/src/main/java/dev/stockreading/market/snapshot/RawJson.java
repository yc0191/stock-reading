package dev.stockreading.market.snapshot;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** 응답 원문을 jsonb 컬럼에 넣을 JSON 텍스트로 바꾼다. */
final class RawJson {

    private RawJson() {
    }

    /**
     * trim 외에 가공하지 않는다. 원문이 JSON이 아니면(오류 페이지 등) 문자열 값으로 감싸서
     * 저장하고, 원문은 {@code #>> '{}'}로 그대로 꺼낼 수 있다. 비어 있으면 null이다.
     */
    static String toJsonb(ObjectMapper mapper, String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = raw.trim();
        try {
            mapper.readTree(trimmed);
            return trimmed;
        } catch (JacksonException e) {
            return mapper.writeValueAsString(trimmed);
        }
    }
}

package dev.stockreading.news.trace;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import jakarta.servlet.FilterChain;

/** 내부 조회 API 요청을 처리하는 동안만 요청 헤더 X-Correlation-Id 가 로그 문맥에 있는지 지킨다. */
class CorrelationIdHttpFilterTest {

    private final CorrelationIdHttpFilter filter = new CorrelationIdHttpFilter();
    private final List<String> seen = new ArrayList<>();
    private final FilterChain chain = (request, response) -> seen.add(MDC.get(CorrelationId.KEY));

    @AfterEach
    void clear() {
        MDC.clear();
    }

    @Test
    void theHeaderIsInTheLogContextWhileHandlingAndRemovedAfter() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/internal/any");
        request.addHeader("X-Correlation-Id", "move-2026-11-13T10:00:00+09:00");

        filter.doFilter(request, new MockHttpServletResponse(), chain);

        assertThat(seen).containsExactly("move-2026-11-13T10:00:00+09:00");
        assertThat(MDC.get(CorrelationId.KEY)).isNull();
    }

    @Test
    void aRequestWithoutTheHeaderDoesNotKeepThePreviousValue() throws Exception {
        // 요청 스레드는 재사용되므로 앞 값이 남으면 엉뚱한 메일의 로그로 검색된다.
        MDC.put(CorrelationId.KEY, "previous");

        filter.doFilter(new MockHttpServletRequest("POST", "/internal/any"), new MockHttpServletResponse(), chain);

        assertThat(seen).containsExactly((String) null);
    }

    @Test
    void theValueIsRemovedEvenWhenHandlingFails() {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/internal/any");
        request.addHeader("X-Correlation-Id", "abc");
        FilterChain failing = (req, res) -> {
            throw new IllegalStateException("db down");
        };

        try {
            filter.doFilter(request, new MockHttpServletResponse(), failing);
        } catch (Exception expected) {
            // 실패는 그대로 올라가고, 값만 지워졌는지 본다.
        }

        assertThat(MDC.get(CorrelationId.KEY)).isNull();
    }
}

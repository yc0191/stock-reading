package dev.stockreading.news.trace;

import java.io.IOException;

import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * 내부 조회 API 요청 하나를 처리하는 동안 요청 헤더 X-Correlation-Id 값을 로그 문맥(MDC)에 둔다.
 * Kafka 메시지 처리와 같은 키를 써서 로그 패턴의 %X{correlationId} 가 그대로 찍는다.
 */
@Component
public class CorrelationIdHttpFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String id = request.getHeader(CorrelationId.HTTP_HEADER);
        // 요청 스레드는 재사용되므로 앞 요청의 값이 남지 않게 값이 없어도 지우고, 끝나면 반드시 지운다.
        if (id == null || id.isBlank()) {
            MDC.remove(CorrelationId.KEY);
        } else {
            MDC.put(CorrelationId.KEY, id);
        }
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(CorrelationId.KEY);
        }
    }
}

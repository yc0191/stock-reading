package dev.stockreading.news.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 내부 조회 API 입구의 요청 검사(빠진 칸, 읽을 수 없는 시각, 상한 0, 거꾸로 된 기간)에 걸린 요청을 400 으로 답하게 한다.
 * Kafka 요청에서는 같은 경우 응답하지 않는데, HTTP 에서 500 으로 답하면 부르는 쪽이 고쳐지지 않을 요청을 다시 보내기 때문이다.
 * 그 밖의 예외는 그대로 두어 Boot 기본 오류 응답(500, 메시지 없음)이 된다.
 */
@RestControllerAdvice
public class InvalidRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(InvalidRequestHandler.class);

    @ExceptionHandler(InvalidRequestException.class)
    ResponseEntity<Void> invalid(InvalidRequestException e) {
        // 메시지에는 jobId·칸 이름만 담기므로 그대로 남긴다. 요청 본문은 보유 종목이 드러나 남기지 않는다.
        log.warn("invalid internal api request {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
    }
}

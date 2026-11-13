package dev.stockreading.market.api;

/**
 * 내부 조회 API 입구에서 요청 칸을 검사하다 잘못을 찾으면 던진다. 400 처리기는 이 예외만 400 으로 바꾼다. 계산 안쪽에서 나는
 * 다른 예외까지 400 으로 바꾸면 서버 쪽 결함이 부르는 쪽 잘못으로 숨기 때문이다.
 */
public class InvalidRequestException extends RuntimeException {

    InvalidRequestException(String message) {
        super(message);
    }
}

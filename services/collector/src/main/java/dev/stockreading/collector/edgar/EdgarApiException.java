package dev.stockreading.collector.edgar;

// 메시지에는 실패 종류와 HTTP 상태만 넣는다. 요청 헤더(User-Agent)와 티커는 넣지 않는다.
public class EdgarApiException extends RuntimeException {

    private final EdgarFailure failure;
    private final Integer httpStatus;

    public EdgarApiException(EdgarFailure failure, Integer httpStatus) {
        super("EDGAR 호출 실패 failure=" + failure + (httpStatus == null ? "" : " http=" + httpStatus));
        this.failure = failure;
        this.httpStatus = httpStatus;
    }

    public EdgarFailure failure() {
        return failure;
    }

    public Integer httpStatus() {
        return httpStatus;
    }
}

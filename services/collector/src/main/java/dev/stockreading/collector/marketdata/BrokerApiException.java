package dev.stockreading.collector.marketdata;

/**
 * 메시지에는 서버가 보낸 문구를 넣지 않는다. responseRaw 는 키, 시크릿, 접근토큰 값을 가린 뒤에만 담는다.
 */
public class BrokerApiException extends RuntimeException {

    private final FailureType failureType;
    private final Integer httpStatus;
    private final String responseCode;
    private final String exceptionType;
    private final String responseRaw;

    public BrokerApiException(FailureType failureType, String message) {
        this(failureType, message, null, null, null, null);
    }

    public BrokerApiException(FailureType failureType, String message, Integer httpStatus, String responseCode,
                              String exceptionType, String responseRaw) {
        super(message);
        this.failureType = failureType;
        this.httpStatus = httpStatus;
        this.responseCode = responseCode;
        this.exceptionType = exceptionType;
        this.responseRaw = responseRaw;
    }

    public FailureType failureType() {
        return failureType;
    }

    public Integer httpStatus() {
        return httpStatus;
    }

    public String responseCode() {
        return responseCode;
    }

    public String exceptionType() {
        return exceptionType;
    }

    public String responseRaw() {
        return responseRaw;
    }
}

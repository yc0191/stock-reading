package dev.stockreading.collector.news;

public class NewsSearchApiException extends RuntimeException {

    public NewsSearchApiException(String message) {
        super(message);
    }

    public NewsSearchApiException(String message, Throwable cause) {
        super(message, cause);
    }
}

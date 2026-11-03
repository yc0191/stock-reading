package dev.stockreading.news.article;

/** 재시도를 모두 소진해 건너뛰는 레코드 한 건. 위치(topic, partition, offset)가 같으면 같은 레코드다. */
record FailedEvent(
        String topic,
        int partition,
        long offset,
        String key,
        String payload,
        String exceptionClass,
        String exceptionMessage,
        int attempts) {
}

package dev.stockreading.collector.insight;

/** market.insight 의 payload. response 는 응답 본문 원문 문자열이다. */
record InsightPayload(String apiType, String target, String response, String scheduledFor, String fetchedAt) {
}

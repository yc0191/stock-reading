package dev.stockreading.collector.insight;

// 장중 조회 사이에 쉬는 동작. 테스트에서 실제로 기다리지 않도록 인터페이스 뒤에 둔다.
interface InsightCallPause {

    void pause(long millis);
}

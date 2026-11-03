package dev.stockreading.collector.news;

// 검색 호출 사이에 쉬는 동작. 테스트에서 실제로 기다리지 않도록 인터페이스 뒤에 둔다.
interface NewsCallPause {

    void pause(long millis);
}

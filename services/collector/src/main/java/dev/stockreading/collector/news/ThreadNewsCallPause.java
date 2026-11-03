package dev.stockreading.collector.news;

import org.springframework.stereotype.Component;

@Component
class ThreadNewsCallPause implements NewsCallPause {

    @Override
    public void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // 종료 신호를 잃지 않도록 중단 표시를 되돌려 놓고, 남은 대기는 건너뛴다.
            Thread.currentThread().interrupt();
        }
    }
}

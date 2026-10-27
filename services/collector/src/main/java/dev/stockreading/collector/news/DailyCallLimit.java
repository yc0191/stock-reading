package dev.stockreading.collector.news;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;

// 검색 API 의 하루 호출 한도를 넘지 않게 세는 가드. 한도는 제공자 쪽 하루 기준이라 한국 시간 자정에 초기화한다.
// 카운터는 메모리에만 있어서 재시작하면 0 으로 돌아간다. 하루에 여러 번 재시작하면 실제 호출이 상한을 넘을 수
// 있으며, 재시작에도 유지되는 카운터(Redis, ADR-008)는 이후 단계의 일이다.
final class DailyCallLimit {

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");

    private final int limit;
    private final Clock clock;
    private LocalDate day;
    private int count;

    DailyCallLimit(int limit, Clock clock) {
        this.limit = limit;
        this.clock = clock;
    }

    /** 호출 한 번을 쓸 수 있으면 세고 true, 오늘 상한을 다 썼으면 세지 않고 false. */
    synchronized boolean tryAcquire() {
        LocalDate today = LocalDate.ofInstant(clock.instant(), KST);
        if (!today.equals(day)) {
            day = today;
            count = 0;
        }
        if (count >= limit) {
            return false;
        }
        count++;
        return true;
    }
}

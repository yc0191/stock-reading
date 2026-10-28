package dev.stockreading.market.snapshot;

import java.time.OffsetDateTime;

/** 날짜 파티션 생성을 DB에 맡기는 포트. 테스트에서 대역으로 바꾼다. */
interface PartitionPort {

    /** 이미 있으면 아무것도 하지 않고, 동시에 만들려 해도 한 번만 만든다. */
    void createIfAbsent(String name, OffsetDateTime from, OffsetDateTime to);
}

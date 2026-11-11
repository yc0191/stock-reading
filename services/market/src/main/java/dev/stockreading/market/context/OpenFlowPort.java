package dev.stockreading.market.context;

import java.time.OffsetDateTime;
import java.util.Optional;

/** 개장 흐름 계산에 쓸 저장된 시세 조회. 테스트에서 바꿀 수 있게 포트로 둔다. */
interface OpenFlowPort {

    /** [since, asOf] 안에서 시세 시각이 가장 이른 스냅샷. 받은 시각이 asOf 뒤인 것은 쓰지 않는다. */
    Optional<Snapshot> first(String symbol, OffsetDateTime since, OffsetDateTime asOf);

    /** [since, asOf] 안에서 시세 시각이 가장 늦은 스냅샷. 받은 시각이 asOf 뒤인 것은 쓰지 않는다. */
    Optional<Snapshot> last(String symbol, OffsetDateTime since, OffsetDateTime asOf);

    record Snapshot(OffsetDateTime asOf, long price, String raw) {
    }
}

package dev.stockreading.market.members;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

/** 이력 행들에서 판의 시간 범위 안에 반영된 변경을 찾는다. DB와 무관한 계산이라 따로 둔다. */
final class MemberChangeFinder {

    private MemberChangeFinder() {
    }

    /** 이력의 한 행. appliedAt은 이력에 반영한 시각이다. */
    record Row(OffsetDateTime appliedAt, LocalDate firstSeenOn, LocalDate lastSeenOn, List<Member> members) {
    }

    /**
     * @param rowsNewestFirst 기준 시각까지 반영된 이력(최신 행이 앞)
     * @param since 시간 범위의 시작. 반영 시각이 이 값 이상인 행이 범위 안이다
     * @return 범위 안 첫 변경 직전 구성과 마지막 구성의 차이. 범위 안에 변경이 없거나 차이가 없으면 빈 값
     */
    static Optional<MemberChanges> find(List<Row> rowsNewestFirst, OffsetDateTime since) {
        int inRange = 0;
        while (inRange < rowsNewestFirst.size() && !rowsNewestFirst.get(inRange).appliedAt().isBefore(since)) {
            inRange++;
        }
        // 범위 안 가장 오래된 행의 앞 행이 "바뀌기 전 구성"이다. 이력 전체가 범위 안이면 가장 오래된 행(처음 받은 구성)이
        // 기준이고, 행이 하나뿐이면 처음 받은 구성이라 변경이 아니다.
        int beforeIndex = Math.min(inRange, rowsNewestFirst.size() - 1);
        if (inRange == 0 || beforeIndex == 0) {
            return Optional.empty();
        }
        Row latest = rowsNewestFirst.get(0);
        Row before = rowsNewestFirst.get(beforeIndex);
        return MemberChanges.between(before.members(), latest.members(), before.lastSeenOn(), latest.firstSeenOn());
    }
}

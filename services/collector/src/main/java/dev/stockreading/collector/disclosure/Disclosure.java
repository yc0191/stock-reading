package dev.stockreading.collector.disclosure;

import java.time.LocalDate;

// 접수번호는 공시마다 유일해서 중복 방지 키로 쓴다. 시각은 없고 날짜(receivedDate)만 제공된다.
public record Disclosure(
        String corpCode,
        String corpName,
        String stockCode,
        String corpClass,
        String reportName,
        String receiptNo,
        String filerName,
        LocalDate receivedDate,
        String remark) {
}

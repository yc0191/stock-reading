package dev.stockreading.collector.edgar;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * 공시 한 건의 메타데이터. 본문은 담지 않는다.
 * acceptedAt 은 SEC 접수 시각(UTC, 초 단위)이고, items 는 8-K 항목 번호(6-K 등 항목이 없는 형식은 빈 목록)다.
 */
public record Filing(
        String accessionNumber,
        String form,
        OffsetDateTime acceptedAt,
        LocalDate filingDate,
        List<String> items,
        String url) {
}

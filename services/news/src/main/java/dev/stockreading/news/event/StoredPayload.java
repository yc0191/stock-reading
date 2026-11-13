package dev.stockreading.news.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * news.stored 의 payload. 수집 완료의 칸을 옮기고 그 기사들의 처리 결과별 개수를 더한다.
 * researchJobId 는 재검색 완료에만 있는 선택 필드라 없으면 직렬화에서 빠진다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record StoredPayload(
        String collectionEventId,
        String jobType,
        String scheduledFor,
        String startedAt,
        List<String> incompleteSymbols,
        String researchJobId,
        int articleCount,
        int storedCount,
        int mergedCount,
        int failedCount,
        List<String> unprocessedEventIds) {
}

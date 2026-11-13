package dev.stockreading.collector.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * researchJobId 는 재검색 완료에만 있는 선택 필드라, 없으면 직렬화에서 빠져야 스키마(type: string)를 지킨다.
 * articleEventIds 는 이번 수집에서 전송을 확정한 기사 이벤트의 eventId 이며, 뉴스처리기가 저장 끝을 판단하는 데 쓴다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CollectionCompletedPayload(
        String jobType,
        String scheduledFor,
        String startedAt,
        String finishedAt,
        int publishedCount,
        List<String> articleEventIds,
        List<String> incompleteSymbols,
        String researchJobId) {
}

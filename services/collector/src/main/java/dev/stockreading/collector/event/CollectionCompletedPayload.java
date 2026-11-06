package dev.stockreading.collector.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;

/** researchJobId 는 재검색 완료에만 있는 선택 필드라, 없으면 직렬화에서 빠져야 스키마(type: string)를 지킨다. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CollectionCompletedPayload(
        String jobType,
        String scheduledFor,
        String startedAt,
        String finishedAt,
        int publishedCount,
        List<String> incompleteSymbols,
        String researchJobId) {

    public CollectionCompletedPayload(String jobType, String scheduledFor, String startedAt, String finishedAt,
            int publishedCount, List<String> incompleteSymbols) {
        this(jobType, scheduledFor, startedAt, finishedAt, publishedCount, incompleteSymbols, null);
    }
}

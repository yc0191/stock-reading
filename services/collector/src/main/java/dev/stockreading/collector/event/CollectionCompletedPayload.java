package dev.stockreading.collector.event;

import java.util.List;

public record CollectionCompletedPayload(
        String jobType,
        String scheduledFor,
        String startedAt,
        String finishedAt,
        int publishedCount,
        List<String> incompleteSymbols) {
}

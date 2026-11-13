package dev.stockreading.collector.event;

import java.util.List;

/** daily.collection.completed 의 본문. 데일리 판(아침 판, 마감 후 판) 하나의 시작 신호다. */
public record DailyCollectionPayload(
        String edition,
        String status,
        String scheduledFor,
        List<String> missingSources) {
}

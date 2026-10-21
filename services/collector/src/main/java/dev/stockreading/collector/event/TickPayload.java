package dev.stockreading.collector.event;

public record TickPayload(String jobType, String scheduledFor) {
}

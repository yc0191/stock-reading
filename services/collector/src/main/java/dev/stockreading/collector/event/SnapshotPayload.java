package dev.stockreading.collector.event;

public record SnapshotPayload(String symbol, long price, String asOf, String source) {
}

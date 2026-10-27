package dev.stockreading.collector.event;

import com.fasterxml.jackson.annotation.JsonInclude;

/** raw는 선택 필드다. 없으면 직렬화에서 빠져야 스키마(type: string)를 지킨다. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SnapshotPayload(String symbol, long price, String asOf, String source, String raw) {

    public SnapshotPayload(String symbol, long price, String asOf, String source) {
        this(symbol, price, asOf, source, null);
    }
}

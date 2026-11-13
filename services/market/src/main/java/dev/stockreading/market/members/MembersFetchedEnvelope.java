package dev.stockreading.market.members;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** etf.members.fetched 의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
record MembersFetchedEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        Payload payload) {

    /** response 는 운용사 응답 원문이다. 받기에 실패하면 members 는 null 이고 failure 에 사유가 있다. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record Payload(String etfSymbol, String baseDate, String fetchedOn, String fetchedAt, int httpStatus,
            String response, List<Member> members, String failure) {
    }
}

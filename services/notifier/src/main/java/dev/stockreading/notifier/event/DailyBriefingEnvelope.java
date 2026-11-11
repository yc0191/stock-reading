package dev.stockreading.notifier.event;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** briefing.daily.generated v2의 소비자 쪽 모델. 모르는 필드는 무시한다(호환 규칙, ADR-010). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DailyBriefingEnvelope(
        String eventId,
        String eventType,
        int schemaVersion,
        String occurredAt,
        String producedAt,
        String correlationId,
        Payload payload) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Payload(
            // 이 판을 받을 멤버 번호. v1 이벤트에는 없어 null이다(받는 사람을 알 수 없어 보내지 않는다).
            Long memberId,
            String edition,
            String rangeStart,
            String rangeEnd,
            String text,
            List<String> missingItems,
            String generatedBy,
            String html,
            List<Image> images) {
    }

    /** html이 cid:로 가리키는 인라인 그림. data는 base64 문자열이다. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Image(String contentId, String contentType, String data) {
    }
}

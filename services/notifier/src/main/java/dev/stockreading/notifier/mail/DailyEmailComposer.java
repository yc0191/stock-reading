package dev.stockreading.notifier.mail;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;

import org.springframework.stereotype.Component;

import dev.stockreading.notifier.NotifierProperties;
import dev.stockreading.notifier.event.DailyBriefingEnvelope;

/** 데일리 브리핑 한 판으로 이메일 한 통을 만든다. 이벤트에 개인 정보가 없으므로 본문은 받은 글을 그대로 쓴다. */
@Component
public class DailyEmailComposer {

    private final String subjectPrefix;

    public DailyEmailComposer(NotifierProperties properties) {
        this.subjectPrefix = properties.subjectPrefix() == null ? "" : properties.subjectPrefix().trim();
    }

    /** @throws java.time.format.DateTimeParseException rangeEnd가 날짜·시각 형식이 아닐 때 */
    public EmailMessage compose(DailyBriefingEnvelope daily) {
        DailyBriefingEnvelope.Payload payload = daily.payload();
        // 판의 날짜는 이번 발송 시각(rangeEnd)의 날짜다. 시각에 붙은 오프셋(한국 시간) 그대로 읽는다.
        LocalDate date = OffsetDateTime.parse(payload.rangeEnd()).toLocalDate();
        String core = "%s 데일리 브리핑 %s".formatted(editionLabel(payload.edition()), date);
        String subject = subjectPrefix.isEmpty() ? core : subjectPrefix + " " + core;

        String body = payload.text()
                + "\n\n----------------------------------------"
                + "\n자료 없음 항목: " + missingItems(payload.missingItems())
                + "\n다룬 범위: " + payload.rangeStart() + " ~ " + payload.rangeEnd()
                + "\n브리핑 ID: " + daily.eventId()
                + "\n추적 ID: " + daily.correlationId()
                + "\n생성 방식: " + payload.generatedBy();
        return new EmailMessage(subject, body);
    }

    private static String editionLabel(String edition) {
        return switch (edition) {
            case "morning" -> "아침 판";
            case "post-close" -> "마감 후 판";
            // 스키마가 막지만, 새 판 종류가 먼저 들어와도 메일은 나가도록 원래 값을 보여 준다.
            default -> edition;
        };
    }

    private static String missingItems(List<String> items) {
        return items == null || items.isEmpty() ? "없음" : String.join(", ", items);
    }
}

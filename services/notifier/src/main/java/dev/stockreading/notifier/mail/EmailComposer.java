package dev.stockreading.notifier.mail;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.stereotype.Component;

import dev.stockreading.notifier.NotifierProperties;
import dev.stockreading.notifier.event.BriefingEnvelope;

/** 브리핑으로 이메일을 만든다. 공개 정보만 쓴다: 보유 종목·수량은 넣지 않는다. */
@Component
public class EmailComposer {

    /** 급변동 메일의 보낸 사람 이름. 데일리 메일과 받은편지함에서 구분되게 한다. */
    static final String FROM_NAME = "변동 감지";

    private final String subjectPrefix;

    public EmailComposer(NotifierProperties properties) {
        this.subjectPrefix = properties.subjectPrefix() == null ? "" : properties.subjectPrefix().trim();
    }

    public EmailMessage compose(BriefingEnvelope briefing) {
        BriefingEnvelope.Payload payload = briefing.payload();
        String direction = payload.changePercent().compareTo(BigDecimal.ZERO) > 0 ? "상승" : "하락";
        // 이름 칸은 선택이라 없거나 비면 코드만 쓴다.
        String label = payload.name() == null || payload.name().isBlank()
                ? payload.symbol() : payload.name().trim() + "(" + payload.symbol() + ")";
        String core = "[%s] %s %+.2f%% 브리핑".formatted(label, direction, payload.changePercent());
        String subject = subjectPrefix.isEmpty() ? core : subjectPrefix + " " + core;

        String body = payload.text()
                + "\n\n----------------------------------------"
                + "\n브리핑 ID: " + briefing.eventId()
                + "\n추적 ID: " + briefing.correlationId()
                + "\n생성 방식: " + payload.generatedBy();
        return new EmailMessage(subject, body, null, List.of(), FROM_NAME);
    }
}

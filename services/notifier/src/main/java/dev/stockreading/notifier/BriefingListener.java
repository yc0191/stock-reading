package dev.stockreading.notifier;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.notifier.event.BriefingEnvelope;
import dev.stockreading.notifier.mail.EmailComposer;
import dev.stockreading.notifier.mail.EmailMessage;
import dev.stockreading.notifier.mail.MailSenderPort;
import dev.stockreading.notifier.recipient.Recipient;
import dev.stockreading.notifier.recipient.RecipientPort;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/** briefing.generated -> 이메일. 최소 한 번 처리: 발송이 성공한 뒤에만 오프셋을 커밋한다. */
@Component
class BriefingListener {

    private static final Logger log = LoggerFactory.getLogger(BriefingListener.class);
    private static final int MAX_REMEMBERED = 1_000;

    private final ObjectMapper mapper;
    private final EmailComposer composer;
    private final MailSenderPort mailSender;
    private final RecipientPort recipients;

    // v0: 메모리 기반 중복 방지(실행 중에만 유지). 이후 DB 기록으로 대체한다. 키는 이벤트와 받는 사람의 조합이라,
    // 여러 사람 중 일부만 보낸 채 실패해 다시 처리해도 이미 받은 사람에게는 다시 보내지 않는다.
    private final Set<String> sentBriefings = ConcurrentHashMap.newKeySet();

    BriefingListener(ObjectMapper mapper, EmailComposer composer, MailSenderPort mailSender,
            RecipientPort recipients) {
        this.mapper = mapper;
        this.composer = composer;
        this.mailSender = mailSender;
        this.recipients = recipients;
    }

    @KafkaListener(topics = Topics.BRIEFING_GENERATED, groupId = "notifier")
    void onBriefing(String message) {
        BriefingEnvelope briefing;
        try {
            briefing = mapper.readValue(message, BriefingEnvelope.class);
        } catch (JacksonException error) {
            // 형식이 잘못된 메시지는 재시도해도 유효해지지 않아서, 재시도하면 같은 실패만 반복한다.
            log.error("skipping malformed briefing.generated message: {}", error.getOriginalMessage());
            return;
        }

        // 받는 사람은 보내기 직전에 고른다. 없으면 보내지 않는다(처리는 끝난 것으로 본다).
        List<Recipient> targets = recipients.forMove(briefing.payload().symbol());
        if (targets.isEmpty()) {
            log.warn("no recipients for briefing; not emailed eventId={} symbol={}", briefing.eventId(),
                    briefing.payload().symbol());
            return;
        }

        EmailMessage email = composer.compose(briefing);
        for (Recipient target : targets) {
            String key = briefing.eventId() + ":" + target.mailId();
            if (sentBriefings.contains(key)) {
                log.info("duplicate briefing ignored eventId={} mailId={}", briefing.eventId(), target.mailId());
                continue;
            }
            mailSender.send(target.address(), email); // 실패하면 예외를 던진다 -> Kafka 에러 핸들러가 재시도한다

            if (sentBriefings.size() >= MAX_REMEMBERED) {
                sentBriefings.clear();
            }
            sentBriefings.add(key);
        }
    }
}

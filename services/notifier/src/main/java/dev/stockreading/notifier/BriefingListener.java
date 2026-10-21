package dev.stockreading.notifier;

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
    private final SendRateLimiter rateLimiter;

    // v0: 메모리 기반 중복 방지(실행 중에만 유지). 이후 DB 기록으로 대체한다.
    private final Set<String> sentBriefings = ConcurrentHashMap.newKeySet();

    BriefingListener(ObjectMapper mapper, EmailComposer composer, MailSenderPort mailSender, SendRateLimiter rateLimiter) {
        this.mapper = mapper;
        this.composer = composer;
        this.mailSender = mailSender;
        this.rateLimiter = rateLimiter;
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

        if (sentBriefings.contains(briefing.eventId())) {
            log.info("duplicate briefing ignored eventId={}", briefing.eventId());
            return;
        }
        if (!rateLimiter.hasCapacity()) {
            log.warn("hourly email limit reached; briefing not emailed eventId={} symbol={}",
                    briefing.eventId(), briefing.payload().symbol());
            return;
        }

        EmailMessage email = composer.compose(briefing);
        mailSender.send(email); // 실패하면 예외를 던진다 -> Kafka 에러 핸들러가 재시도한다
        rateLimiter.recordSent();

        if (sentBriefings.size() >= MAX_REMEMBERED) {
            sentBriefings.clear();
        }
        sentBriefings.add(briefing.eventId());
    }
}

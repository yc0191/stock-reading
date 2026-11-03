package dev.stockreading.notifier;

import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.stockreading.notifier.event.DailyBriefingEnvelope;
import dev.stockreading.notifier.mail.DailyEmailComposer;
import dev.stockreading.notifier.mail.EmailMessage;
import dev.stockreading.notifier.mail.MailSenderPort;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * briefing.daily.generated -> 판마다 이메일 한 통. 최소 한 번 처리: 발송이 성공한 뒤에만 오프셋을 커밋한다.
 * 데일리 메일은 하루 정해진 통이라 시간당 한도로 막지 않고, 보낸 수만 한도에 더한다(그 뒤 건별 메일이 막힐 수 있다).
 */
@Component
class DailyBriefingListener {

    private static final Logger log = LoggerFactory.getLogger(DailyBriefingListener.class);
    private static final int MAX_REMEMBERED = 1_000;

    private final ObjectMapper mapper;
    private final DailyEmailComposer composer;
    private final MailSenderPort mailSender;
    private final SendRateLimiter rateLimiter;

    // 건별 메일과 같은 메모리 기반 중복 방지(실행 중에만 유지).
    private final Set<String> sentDailies = ConcurrentHashMap.newKeySet();

    DailyBriefingListener(ObjectMapper mapper, DailyEmailComposer composer, MailSenderPort mailSender,
            SendRateLimiter rateLimiter) {
        this.mapper = mapper;
        this.composer = composer;
        this.mailSender = mailSender;
        this.rateLimiter = rateLimiter;
    }

    @KafkaListener(topics = Topics.DAILY_BRIEFING_GENERATED, groupId = "notifier")
    void onDailyBriefing(String message) {
        DailyBriefingEnvelope daily;
        EmailMessage email;
        try {
            daily = mapper.readValue(message, DailyBriefingEnvelope.class);
            if (!hasRequiredFields(daily)) {
                log.error("skipping briefing.daily.generated message without required fields eventId={}",
                        daily.eventId());
                return;
            }
            email = composer.compose(daily);
        } catch (JacksonException error) {
            // 형식이 잘못된 메시지는 재시도해도 유효해지지 않아서, 재시도하면 같은 실패만 반복한다.
            log.error("skipping malformed briefing.daily.generated message: {}", error.getOriginalMessage());
            return;
        } catch (DateTimeParseException error) {
            log.error("skipping briefing.daily.generated message with invalid rangeEnd: {}", error.getMessage());
            return;
        }

        if (sentDailies.contains(daily.eventId())) {
            log.info("duplicate daily briefing ignored eventId={}", daily.eventId());
            return;
        }
        mailSender.send(email); // 실패하면 예외를 던진다 -> Kafka 에러 핸들러가 재시도한다
        rateLimiter.recordSent();

        if (sentDailies.size() >= MAX_REMEMBERED) {
            sentDailies.clear();
        }
        sentDailies.add(daily.eventId());
    }

    private static boolean hasRequiredFields(DailyBriefingEnvelope daily) {
        DailyBriefingEnvelope.Payload payload = daily.payload();
        return daily.eventId() != null && payload != null
                && payload.edition() != null && payload.rangeEnd() != null
                && payload.text() != null && !payload.text().isBlank();
    }
}
